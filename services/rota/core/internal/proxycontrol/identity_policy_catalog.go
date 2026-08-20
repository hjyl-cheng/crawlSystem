package proxycontrol

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

//go:embed identity_policy_catalog.generated.json
var embeddedIdentityPolicyCatalog []byte

type IdentityPolicyCatalog struct {
	SchemaVersion  int
	CatalogVersion int
	WorkloadScope  string
	Digest         string
	Policies       map[string]IdentityPolicy
}

func (c IdentityPolicyCatalog) PoliciesForWorkloadScope(expected string) (map[string]IdentityPolicy, error) {
	expected = strings.TrimSpace(expected)
	if expected == "" || c.WorkloadScope != expected {
		return nil, fmt.Errorf("identity policy workload scope mismatch: %q != %q", c.WorkloadScope, expected)
	}
	policies := make(map[string]IdentityPolicy, len(c.Policies))
	for id, value := range c.Policies {
		value.AllowedProxyTags = append([]string(nil), value.AllowedProxyTags...)
		policies[id] = value
	}
	return policies, nil
}

type identityPolicyCatalogDocument struct {
	SchemaVersion  int                      `json:"schema_version"`
	CatalogVersion int                      `json:"catalog_version"`
	WorkloadScope  string                   `json:"workload_scope"`
	Policies       []identityPolicyDocument `json:"policies"`
}

type identityPolicyDocument struct {
	ID                         string   `json:"id"`
	Version                    int      `json:"version"`
	Hash                       string   `json:"hash"`
	Role                       string   `json:"role"`
	YouTubeLanguage            string   `json:"youtube_language"`
	YouTubeControlLanguage     string   `json:"youtube_control_language"`
	YouTubeCountry             string   `json:"youtube_country"`
	BrowserProfileTimezone     string   `json:"browser_profile_timezone"`
	RequiredEgressCountry      string   `json:"required_egress_country"`
	AllowedProxyTags           []string `json:"allowed_proxy_tags"`
	GeoFreshnessWindowSeconds  int64    `json:"geo_freshness_window_seconds"`
	AttemptSafetyWindowSeconds int64    `json:"attempt_safety_window_seconds"`
}

type identityPolicyHashDocument struct {
	SchemaVersion              int      `json:"schema_version"`
	ID                         string   `json:"id"`
	Version                    int      `json:"version"`
	Role                       string   `json:"role"`
	YouTubeLanguage            string   `json:"youtube_language"`
	YouTubeControlLanguage     string   `json:"youtube_control_language"`
	YouTubeCountry             string   `json:"youtube_country"`
	BrowserProfileTimezone     string   `json:"browser_profile_timezone"`
	RequiredEgressCountry      string   `json:"required_egress_country"`
	AllowedProxyTags           []string `json:"allowed_proxy_tags"`
	GeoFreshnessWindowSeconds  int64    `json:"geo_freshness_window_seconds"`
	AttemptSafetyWindowSeconds int64    `json:"attempt_safety_window_seconds"`
}

func LoadEmbeddedIdentityPolicyCatalog() (IdentityPolicyCatalog, error) {
	var document identityPolicyCatalogDocument
	if err := json.Unmarshal(embeddedIdentityPolicyCatalog, &document); err != nil {
		return IdentityPolicyCatalog{}, fmt.Errorf("decode embedded identity policy catalog: %w", err)
	}
	if document.SchemaVersion != 1 || document.CatalogVersion <= 0 || strings.TrimSpace(document.WorkloadScope) == "" {
		return IdentityPolicyCatalog{}, fmt.Errorf("invalid identity policy catalog header")
	}

	catalog := IdentityPolicyCatalog{
		SchemaVersion:  document.SchemaVersion,
		CatalogVersion: document.CatalogVersion,
		WorkloadScope:  strings.TrimSpace(document.WorkloadScope),
		Digest:         sha256Digest(embeddedIdentityPolicyCatalog),
		Policies:       make(map[string]IdentityPolicy, len(document.Policies)),
	}
	policyByRole := make(map[string]string, len(document.Policies))
	for _, value := range document.Policies {
		policy, err := identityPolicyFromDocument(document.SchemaVersion, value)
		if err != nil {
			return IdentityPolicyCatalog{}, err
		}
		if _, exists := catalog.Policies[policy.ID]; exists {
			return IdentityPolicyCatalog{}, fmt.Errorf("duplicate identity policy %q", policy.ID)
		}
		if existingID, exists := policyByRole[policy.Role]; exists {
			return IdentityPolicyCatalog{}, fmt.Errorf(
				"identity policies %q and %q both target role %q",
				existingID, policy.ID, policy.Role,
			)
		}
		catalog.Policies[policy.ID] = policy
		policyByRole[policy.Role] = policy.ID
	}
	if len(catalog.Policies) == 0 {
		return IdentityPolicyCatalog{}, fmt.Errorf("identity policy catalog is empty")
	}
	return catalog, nil
}

func identityPolicyFromDocument(schemaVersion int, value identityPolicyDocument) (IdentityPolicy, error) {
	value.ID = strings.TrimSpace(value.ID)
	value.Role = strings.ToLower(strings.TrimSpace(value.Role))
	value.YouTubeLanguage = strings.TrimSpace(value.YouTubeLanguage)
	value.YouTubeControlLanguage = strings.TrimSpace(value.YouTubeControlLanguage)
	value.YouTubeCountry = strings.ToUpper(strings.TrimSpace(value.YouTubeCountry))
	value.BrowserProfileTimezone = strings.TrimSpace(value.BrowserProfileTimezone)
	value.RequiredEgressCountry = strings.ToUpper(strings.TrimSpace(value.RequiredEgressCountry))
	value.AllowedProxyTags = normalizePolicyTags(value.AllowedProxyTags)

	if value.ID == "" || value.Version <= 0 || !validCatalogRole(value.Role) ||
		value.YouTubeLanguage == "" || value.YouTubeControlLanguage == "" ||
		len(value.YouTubeCountry) != 2 || value.BrowserProfileTimezone == "" ||
		len(value.RequiredEgressCountry) != 2 || len(value.AllowedProxyTags) == 0 ||
		value.GeoFreshnessWindowSeconds <= 0 || value.AttemptSafetyWindowSeconds <= 0 {
		return IdentityPolicy{}, fmt.Errorf("identity policy %q is invalid", value.ID)
	}

	hashValue := identityPolicyHashDocument{
		SchemaVersion: schemaVersion,
		ID:            value.ID, Version: value.Version, Role: value.Role,
		YouTubeLanguage:            value.YouTubeLanguage,
		YouTubeControlLanguage:     value.YouTubeControlLanguage,
		YouTubeCountry:             value.YouTubeCountry,
		BrowserProfileTimezone:     value.BrowserProfileTimezone,
		RequiredEgressCountry:      value.RequiredEgressCountry,
		AllowedProxyTags:           value.AllowedProxyTags,
		GeoFreshnessWindowSeconds:  value.GeoFreshnessWindowSeconds,
		AttemptSafetyWindowSeconds: value.AttemptSafetyWindowSeconds,
	}
	hashPayload, err := json.Marshal(hashValue)
	if err != nil {
		return IdentityPolicy{}, fmt.Errorf("encode identity policy %q hash: %w", value.ID, err)
	}
	computedHash := sha256Digest(hashPayload)
	if value.Hash != computedHash {
		return IdentityPolicy{}, fmt.Errorf("identity policy %q hash is invalid", value.ID)
	}
	return IdentityPolicy{
		ID: value.ID, Version: value.Version, Hash: computedHash, Role: value.Role,
		YouTubeLanguage:        value.YouTubeLanguage,
		YouTubeControlLanguage: value.YouTubeControlLanguage,
		YouTubeCountry:         value.YouTubeCountry,
		BrowserProfileTimezone: value.BrowserProfileTimezone,
		RequiredEgressCountry:  value.RequiredEgressCountry,
		AllowedProxyTags:       append([]string(nil), value.AllowedProxyTags...),
		GeoFreshnessWindow:     time.Duration(value.GeoFreshnessWindowSeconds) * time.Second,
		AttemptSafetyWindow:    time.Duration(value.AttemptSafetyWindowSeconds) * time.Second,
	}, nil
}

func normalizePolicyTags(values []string) []string {
	tags := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" && !seen[value] {
			seen[value] = true
			tags = append(tags, value)
		}
	}
	slices.Sort(tags)
	return tags
}

func validCatalogRole(role string) bool {
	return role == RoleChannel || role == RoleDiscover || role == RoleQueryQuality
}

func sha256Digest(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}
