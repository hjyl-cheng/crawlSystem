package models

import "time"

// ProxySource represents a remote URL that provides a list of proxies
type ProxySource struct {
	ID                          int        `json:"id"`
	Name                        string     `json:"name"`
	URL                         string     `json:"url"`
	Protocol                    string     `json:"protocol"`
	Enabled                     bool       `json:"enabled"`
	IntervalMinutes             int        `json:"interval_minutes"`
	LastFetchedAt               *time.Time `json:"last_fetched_at,omitempty"`
	LastCount                   int        `json:"last_count"` // newly imported on last fetch
	LastTotal                   int        `json:"last_total"` // non-empty node lines returned on last fetch
	LastSupported               int        `json:"last_supported"`
	LastSkipped                 int        `json:"last_skipped"`
	ActiveCount                 int        `json:"active_count"`
	LastError                   *string    `json:"last_error,omitempty"`
	SuccessfulRefreshGeneration int64      `json:"successful_refresh_generation"`
	LastCompleteRefreshAt       *time.Time `json:"last_complete_refresh_at,omitempty"`
	CleanupEnabled              bool       `json:"cleanup_enabled"` // Opt-in retirement after repeated complete-refresh absence.
	CleanupDays                 int        `json:"cleanup_days"`    // Minimum absence window before retirement eligibility.
	DefaultTags                 []string   `json:"default_tags"`
	CreatedAt                   time.Time  `json:"created_at"`
	UpdatedAt                   time.Time  `json:"updated_at"`
}

// CreateProxySourceRequest is the payload for creating a source
type CreateProxySourceRequest struct {
	Name            string   `json:"name"     validate:"required"`
	URL             string   `json:"url"      validate:"required,url"`
	Protocol        string   `json:"protocol" validate:"required,oneof=auto http https socks4 socks4a socks5 vless vmess trojan shadowsocks hysteria2"`
	Enabled         bool     `json:"enabled"`
	IntervalMinutes int      `json:"interval_minutes" validate:"min=1"`
	DefaultTags     []string `json:"default_tags,omitempty"`
}

// UpdateProxySourceRequest is the payload for updating a source
type UpdateProxySourceRequest struct {
	Name            string    `json:"name"`
	URL             string    `json:"url"`
	Protocol        string    `json:"protocol" validate:"omitempty,oneof=auto http https socks4 socks4a socks5 vless vmess trojan shadowsocks hysteria2"`
	Enabled         *bool     `json:"enabled"`
	IntervalMinutes int       `json:"interval_minutes" validate:"omitempty,min=1"`
	DefaultTags     *[]string `json:"default_tags,omitempty"`
	CleanupEnabled  *bool     `json:"cleanup_enabled,omitempty"`
	CleanupDays     *int      `json:"cleanup_days,omitempty" validate:"omitempty,min=1,max=3650"`
}
