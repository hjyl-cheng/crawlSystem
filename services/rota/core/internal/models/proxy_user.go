package models

import (
	"encoding/json"
	"time"
)

// ProxyUser is a user that authenticates to the proxy server port (8000).
// Each user has a main pool and optional ordered fallback pools.
type ProxyUser struct {
	ID                int       `json:"id"`
	Username          string    `json:"username"`
	PasswordHash      string    `json:"-"` // bcrypt, never in JSON
	Enabled           bool      `json:"enabled"`
	MainPoolID        *int      `json:"main_pool_id,omitempty"`
	FallbackPoolIDs   []int     `json:"fallback_pool_ids"`
	MaxRetries        int       `json:"max_retries"`
	RequestsPerMinute int       `json:"requests_per_minute"` // 0 = no limit
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`

	// Enriched fields (JOIN, not stored)
	MainPoolName string `json:"main_pool_name,omitempty"`
}

// ProxyUserWithPools is ProxyUser + full pool objects for the API
type ProxyUserWithPools struct {
	ProxyUser
	MainPool      *ProxyPool  `json:"main_pool,omitempty"`
	FallbackPools []ProxyPool `json:"fallback_pools"`
}

// CreateProxyUserRequest is the payload for POST /api/v1/proxy-users
type CreateProxyUserRequest struct {
	Username          string `json:"username"             validate:"required"`
	Password          string `json:"password"             validate:"required,min=6"`
	Enabled           bool   `json:"enabled"`
	MainPoolID        *int   `json:"main_pool_id,omitempty"`
	FallbackPoolIDs   []int  `json:"fallback_pool_ids"`
	MaxRetries        int    `json:"max_retries"           validate:"min=1,max=50"`
	RequestsPerMinute int    `json:"requests_per_minute"` // 0 = no limit
}

// UpdateProxyUserRequest is the payload for PUT /api/v1/proxy-users/{id}
type UpdateProxyUserRequest struct {
	Password          string `json:"password,omitempty"`
	Enabled           *bool  `json:"enabled,omitempty"`
	MainPoolID        *int   `json:"main_pool_id"`      // null clears it
	FallbackPoolIDs   []int  `json:"fallback_pool_ids"` // replaces list
	MaxRetries        int    `json:"max_retries,omitempty"`
	RequestsPerMinute *int   `json:"requests_per_minute,omitempty"`

	mainPoolIDSet bool
}

// UnmarshalJSON preserves whether main_pool_id was omitted or explicitly set
// to null, which a plain *int cannot distinguish.
func (r *UpdateProxyUserRequest) UnmarshalJSON(data []byte) error {
	type requestAlias UpdateProxyUserRequest
	var decoded requestAlias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*r = UpdateProxyUserRequest(decoded)
	_, r.mainPoolIDSet = fields["main_pool_id"]
	return nil
}

// HasMainPoolID reports whether an update should replace or clear the main
// Pool. A non-nil value also supports requests assembled directly in Go.
func (r UpdateProxyUserRequest) HasMainPoolID() bool {
	return r.mainPoolIDSet || r.MainPoolID != nil
}

// proxyUserContextKey is used to pass the resolved ProxyUser through request context
type proxyUserContextKey struct{}

// ProxyUserContextKey is the exported key for request context
var ProxyUserContextKey = proxyUserContextKey{}
