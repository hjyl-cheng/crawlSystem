package proxycontrol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEmbeddedIdentityPolicyCatalogIsCanonicalProductionBRV1(t *testing.T) {
	catalog, err := LoadEmbeddedIdentityPolicyCatalog()
	if err != nil {
		t.Fatalf("load embedded identity policy catalog: %v", err)
	}
	if catalog.SchemaVersion != 1 || catalog.CatalogVersion != 1 ||
		catalog.WorkloadScope != "qy-production" || catalog.Digest == "" {
		t.Fatalf("catalog identity = %+v", catalog)
	}

	want := map[string]struct {
		role string
		tag  string
	}{
		"qy-br-channel-anonymous-v1":       {role: RoleChannel, tag: "role:channel"},
		"qy-br-discover-anonymous-v1":      {role: RoleDiscover, tag: "role:discover"},
		"qy-br-query-quality-anonymous-v1": {role: RoleQueryQuality, tag: "role:query_quality"},
	}
	if len(catalog.Policies) != len(want) {
		t.Fatalf("policy count = %d, want %d", len(catalog.Policies), len(want))
	}
	for policyID, expected := range want {
		policy, found := catalog.Policies[policyID]
		if !found {
			t.Fatalf("missing policy %q", policyID)
		}
		if policy.ID != policyID || policy.Version != 1 || policy.Role != expected.role ||
			policy.Hash == "" || policy.YouTubeLanguage != "pt-BR" ||
			policy.YouTubeControlLanguage != "en" || policy.YouTubeCountry != "BR" ||
			policy.BrowserProfileTimezone != "America/Sao_Paulo" ||
			policy.RequiredEgressCountry != "BR" ||
			len(policy.AllowedProxyTags) != 1 || policy.AllowedProxyTags[0] != expected.tag ||
			policy.GeoFreshnessWindow != 24*time.Hour ||
			policy.AttemptSafetyWindow != 2*time.Hour {
			t.Fatalf("policy %q = %+v", policyID, policy)
		}
	}
}

func TestIdentityPolicyCatalogRejectsAWorkloadScopeMismatch(t *testing.T) {
	catalog, err := LoadEmbeddedIdentityPolicyCatalog()
	if err != nil {
		t.Fatalf("load embedded identity policy catalog: %v", err)
	}
	if _, err := catalog.PoliciesForWorkloadScope("qy-test"); err == nil {
		t.Fatal("PoliciesForWorkloadScope() error = nil, want mismatch")
	}
	policies, err := catalog.PoliciesForWorkloadScope("qy-production")
	if err != nil {
		t.Fatalf("PoliciesForWorkloadScope(): %v", err)
	}
	policy := policies["qy-br-channel-anonymous-v1"]
	policy.AllowedProxyTags[0] = "modified"
	if catalog.Policies[policy.ID].AllowedProxyTags[0] != "role:channel" {
		t.Fatal("returned policy map aliases the embedded catalog")
	}
}

func TestIdentityPolicyCatalogRejectsMoreThanOnePolicyForARole(t *testing.T) {
	var document identityPolicyCatalogDocument
	if err := json.Unmarshal(embeddedIdentityPolicyCatalog, &document); err != nil {
		t.Fatalf("decode embedded catalog fixture: %v", err)
	}
	duplicate := document.Policies[0]
	duplicate.ID = "qy-br-channel-anonymous-v2"
	duplicate.Version = 2
	hashPayload, err := json.Marshal(identityPolicyHashDocument{
		SchemaVersion: document.SchemaVersion,
		ID:            duplicate.ID, Version: duplicate.Version, Role: duplicate.Role,
		YouTubeLanguage:            duplicate.YouTubeLanguage,
		YouTubeControlLanguage:     duplicate.YouTubeControlLanguage,
		YouTubeCountry:             duplicate.YouTubeCountry,
		BrowserProfileTimezone:     duplicate.BrowserProfileTimezone,
		RequiredEgressCountry:      duplicate.RequiredEgressCountry,
		AllowedProxyTags:           duplicate.AllowedProxyTags,
		GeoFreshnessWindowSeconds:  duplicate.GeoFreshnessWindowSeconds,
		AttemptSafetyWindowSeconds: duplicate.AttemptSafetyWindowSeconds,
	})
	if err != nil {
		t.Fatalf("encode duplicate policy hash: %v", err)
	}
	duplicate.Hash = sha256Digest(hashPayload)
	document.Policies = append(document.Policies, duplicate)
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("encode duplicate-role catalog: %v", err)
	}

	original := embeddedIdentityPolicyCatalog
	embeddedIdentityPolicyCatalog = encoded
	t.Cleanup(func() { embeddedIdentityPolicyCatalog = original })
	if _, err := LoadEmbeddedIdentityPolicyCatalog(); err == nil {
		t.Fatal("LoadEmbeddedIdentityPolicyCatalog() error = nil, want duplicate role rejection")
	}
}
