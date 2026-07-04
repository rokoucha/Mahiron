package config

import (
	"errors"
	"net/url"
	"os"

	"sigs.k8s.io/yaml"
)

type SystemConfig struct {
	Addresses          []ServerAddress     `json:"addresses"`
	LogLevel           string              `json:"logLevel,omitempty"`
	Observability      ObservabilityConfig `json:"observability,omitempty"`
	MaxConcurrentJobs  int                 `json:"maxConcurrentJobs,omitempty"`
	Jobs               []JobScheduleConfig `json:"jobs,omitempty"`
	DatabasePath       string              `json:"databasePath,omitempty"`
	EpgRetentionDays   int                 `json:"epgRetentionDays,omitempty"`
	EpgRetrievalTime   int                 `json:"epgRetrievalTime,omitempty"`
	EpgStaleAfter      int                 `json:"epgStaleAfter,omitempty"`
	LogoGatherTimeout  int                 `json:"logoGatherTimeout,omitempty"`
	ServiceScanTimeout int                 `json:"serviceScanTimeout,omitempty"`
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
}

type ObservabilitySignal struct {
	Enabled bool `json:"enabled,omitempty"`
}

func LoadAndParseSystemConfig(filePath string) (*SystemConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	config := SystemConfig{
		DatabasePath:       "./db/mahiron.db",
		MaxConcurrentJobs:  1,
		EpgRetentionDays:   3,
		EpgRetrievalTime:   600000,
		EpgStaleAfter:      7200000,
		LogoGatherTimeout:  1200000,
		ServiceScanTimeout: 30000,
	}
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}

	if len(config.Addresses) == 0 {
		config.Addresses = []ServerAddress{
			{
				Http: "localhost:40772",
			},
		}
	}

	for _, addr := range config.Addresses {
		if addr.Http == "" && addr.Unix == "" {
			return nil, errors.New("at least one address is required")
		}
		if addr.Http != "" && addr.Unix != "" {
			return nil, errors.New("only one address type is allowed")
		}
	}

	if config.LogLevel == "" {
		config.LogLevel = "info"
	}
	if config.Observability.ServiceName == "" {
		config.Observability.ServiceName = "mahiron"
	}
	if config.Observability.Endpoint != "" {
		u, err := url.Parse(config.Observability.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, errors.New("observability endpoint must be a URL like http://localhost:4318")
		}
	}

	switch config.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, errors.New("invalid log level")
	}
	if config.MaxConcurrentJobs < 1 || config.MaxConcurrentJobs > 100 {
		return nil, errors.New("maxConcurrentJobs must be between 1 and 100")
	}
	if config.EpgRetentionDays < 0 {
		return nil, errors.New("epgRetentionDays must be >= 0 (0 = unlimited)")
	}
	if config.EpgRetrievalTime < 5000 {
		return nil, errors.New("epgRetrievalTime must be >= 5000")
	}
	if config.EpgStaleAfter <= 0 {
		return nil, errors.New("epgStaleAfter must be > 0")
	}
	if config.LogoGatherTimeout <= 0 {
		return nil, errors.New("logoGatherTimeout must be > 0")
	}
	if config.ServiceScanTimeout < 5000 {
		return nil, errors.New("serviceScanTimeout must be >= 5000")
	}

	return &config, nil
}
