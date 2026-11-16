package config

import (
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

type AgentCredential struct {
	Name      string `json:"name"`
	TokenHash string `json:"tokenHash"`
}

type SystemConfig struct {
	Agents            []AgentCredential   `json:"agents,omitempty"`
	Addresses         []ServerAddress     `json:"addresses"`
	LogLevel          string              `json:"logLevel,omitempty"`
	Observability     ObservabilityConfig `json:"observability,omitempty"`
	MaxConcurrentJobs int                 `json:"maxConcurrentJobs,omitempty"`
	Jobs              []JobScheduleConfig `json:"jobs,omitempty"`
	DatabasePath      string              `json:"databasePath,omitempty"`
	// DataBroadcastEnabled controls the data broadcast API and its cache.
	// It defaults to enabled when omitted.
	DataBroadcastEnabled         *bool  `json:"dataBroadcastEnabled,omitempty"`
	DataBroadcastCachePath       string `json:"dataBroadcastCachePath,omitempty"`
	DataBroadcastCacheBytes      uint64 `json:"dataBroadcastCacheBytes,omitempty"`
	DataBroadcastCacheMaxAgeDays int    `json:"dataBroadcastCacheMaxAgeDays,omitempty"`
	// DataBroadcastSnapshotDisabled turns off persisting PMT/DII sections for
	// provisional /state responses. Enabled (nil or false) by default.
	DataBroadcastSnapshotDisabled    *bool   `json:"dataBroadcastSnapshotDisabled,omitempty"`
	DataBroadcastSnapshotMaxAgeHours int     `json:"dataBroadcastSnapshotMaxAgeHours,omitempty"`
	EpgGatheringMode                 EPGMode `json:"epgGatheringMode,omitempty"`
	EpgUnknownExtendedIdleTime       int     `json:"epgUnknownExtendedIdleTime,omitempty"`
	EpgExtendedProbeInterval         int     `json:"epgExtendedProbeInterval,omitempty"`
	EpgRefreshInterval               int     `json:"epgRefreshInterval,omitempty"`
	EpgRetentionDays                 int     `json:"epgRetentionDays,omitempty"`
	EpgRetrievalTime                 int     `json:"epgRetrievalTime,omitempty"`
	EpgStaleAfter                    int     `json:"epgStaleAfter,omitempty"`
	LogoGatherTimeout                int     `json:"logoGatherTimeout,omitempty"`
	ServiceScanTimeout               int     `json:"serviceScanTimeout,omitempty"`
}

type JobScheduleConfig struct {
	Key      string `json:"key"`
	Schedule string `json:"schedule"`
}

type ServerAddress struct {
	Http string `json:"http,omitempty"`
	Unix string `json:"unix,omitempty"`
}

type ObservabilityConfig struct {
	ServiceName string              `json:"serviceName,omitempty"`
	Endpoint    string              `json:"endpoint,omitempty"`
	Headers     map[string]string   `json:"headers,omitempty"`
	Logs        ObservabilitySignal `json:"logs,omitempty"`
	Traces      ObservabilitySignal `json:"traces,omitempty"`
	Metrics     ObservabilitySignal `json:"metrics,omitempty"`
	// Pprof serves net/http/pprof under /debug/pprof on every configured
	// listener. It stays off unless asked for: the handlers report what the
	// process is doing and holding, and a profile request occupies the server
	// for its whole duration, neither of which belongs on an interface that
	// otherwise only serves the API and the dashboard.
	Pprof ObservabilitySignal `json:"pprof,omitempty"`
}

type ObservabilitySignal struct {
	Enabled bool `json:"enabled,omitempty"`
}

func IsDataBroadcastSnapshotEnabled(config SystemConfig) bool {
	return config.DataBroadcastSnapshotDisabled == nil || !*config.DataBroadcastSnapshotDisabled
}

func IsDataBroadcastEnabled(config SystemConfig) bool {
	return config.DataBroadcastEnabled == nil || *config.DataBroadcastEnabled
}

func LoadAndParseSystemConfig(filePath string) (*SystemConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	config := defaultSystemConfig()
	if err := yaml.Unmarshal(file, &config); err != nil {
		return nil, err
	}
	config.applyDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func defaultSystemConfig() SystemConfig {
	return SystemConfig{
		DatabasePath:                     "./db/mahiron.db",
		DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
		DataBroadcastCacheBytes:          1024 * 1024 * 1024,
		DataBroadcastCacheMaxAgeDays:     14,
		DataBroadcastSnapshotMaxAgeHours: 6,
		MaxConcurrentJobs:                1,
		EpgRefreshInterval:               1800000,
		EpgUnknownExtendedIdleTime:       30000,
		EpgExtendedProbeInterval:         21600000,
		EpgRetentionDays:                 3,
		EpgRetrievalTime:                 600000,
		EpgStaleAfter:                    7200000,
		LogoGatherTimeout:                1200000,
		ServiceScanTimeout:               30000,
	}
}

// applyDefaults fills empty values that also default when explicitly configured.
// Numeric defaults are applied before decoding so explicit zero values survive.
func (config *SystemConfig) applyDefaults() {
	if len(config.Addresses) == 0 {
		config.Addresses = []ServerAddress{
			{
				Http: "localhost:40772",
			},
		}
	}

	if config.LogLevel == "" {
		config.LogLevel = "info"
	}
	if config.Observability.ServiceName == "" {
		config.Observability.ServiceName = "mahiron"
	}
}

func (config SystemConfig) validate() error {
	seenAgents := map[string]bool{}
	seenHashes := map[string]bool{}
	for _, a := range config.Agents {
		hash, err := hex.DecodeString(a.TokenHash)
		if a.Name == "" || seenAgents[a.Name] || err != nil || len(hash) != 32 || seenHashes[string(hash)] {
			return errors.New("agents require unique names and unique SHA-256 token hashes")
		}
		seenAgents[a.Name] = true
		seenHashes[string(hash)] = true
	}

	for _, addr := range config.Addresses {
		if addr.Http == "" && addr.Unix == "" {
			return errors.New("at least one address is required")
		}
		if addr.Http != "" && addr.Unix != "" {
			return errors.New("only one address type is allowed")
		}
	}

	if config.Observability.Endpoint != "" {
		u, err := url.Parse(config.Observability.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errors.New("observability endpoint must be a URL like http://localhost:4318")
		}
	}

	switch config.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("invalid log level")
	}
	if config.EpgRefreshInterval <= 0 {
		return errors.New("epgRefreshInterval must be > 0")
	}
	if config.EpgUnknownExtendedIdleTime < 0 || config.EpgUnknownExtendedIdleTime > 0 && config.EpgUnknownExtendedIdleTime < 1000 || config.EpgExtendedProbeInterval <= 0 {
		return errors.New("epgUnknownExtendedIdleTime must be 0 or >= 1000 and epgExtendedProbeInterval must be > 0")
	}
	if _, err := config.ResolveEPG(); err != nil {
		return err
	}
	if config.MaxConcurrentJobs < 1 || config.MaxConcurrentJobs > 100 {
		return errors.New("maxConcurrentJobs must be between 1 and 100")
	}
	if config.EpgRetentionDays < 0 {
		return errors.New("epgRetentionDays must be >= 0 (0 = unlimited)")
	}
	if config.DataBroadcastCacheMaxAgeDays < 0 {
		return errors.New("dataBroadcastCacheMaxAgeDays must be >= 0 (0 = unlimited)")
	}
	if config.DataBroadcastSnapshotMaxAgeHours < 0 {
		return errors.New("dataBroadcastSnapshotMaxAgeHours must be >= 0 (0 = unlimited)")
	}
	if config.EpgRetrievalTime < 5000 {
		return errors.New("epgRetrievalTime must be >= 5000")
	}
	if config.EpgStaleAfter <= 0 {
		return errors.New("epgStaleAfter must be > 0")
	}
	if config.LogoGatherTimeout <= 0 {
		return errors.New("logoGatherTimeout must be > 0")
	}
	if config.ServiceScanTimeout < 5000 {
		return errors.New("serviceScanTimeout must be >= 5000")
	}

	return nil
}

func (config SystemConfig) EpgUnknownExtendedIdleTimeDuration() time.Duration {
	return time.Duration(config.EpgUnknownExtendedIdleTime) * time.Millisecond
}

func (config SystemConfig) EpgExtendedProbeIntervalDuration() time.Duration {
	return time.Duration(config.EpgExtendedProbeInterval) * time.Millisecond
}

func (config SystemConfig) EpgRefreshIntervalDuration() time.Duration {
	return time.Duration(config.EpgRefreshInterval) * time.Millisecond
}

func (config SystemConfig) EpgRetrievalTimeDuration() time.Duration {
	return time.Duration(config.EpgRetrievalTime) * time.Millisecond
}

// EpgStaleAfterDuration returns the timeout as a duration. Timestamp comparisons
// use EpgStaleAfter directly as int64 milliseconds to avoid duration overflow.
func (config SystemConfig) EpgStaleAfterDuration() time.Duration {
	return time.Duration(config.EpgStaleAfter) * time.Millisecond
}

func (config SystemConfig) LogoGatherTimeoutDuration() time.Duration {
	return time.Duration(config.LogoGatherTimeout) * time.Millisecond
}

func (config SystemConfig) ServiceScanTimeoutDuration() time.Duration {
	return time.Duration(config.ServiceScanTimeout) * time.Millisecond
}
