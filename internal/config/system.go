package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type SystemConfig struct {
	Addresses          []ServerAddress     `json:"addresses"`
	LogLevel           string              `json:"logLevel,omitempty"`
	Observability      ObservabilityConfig `json:"observability,omitempty"`
	JobMaxRunning      int                 `json:"jobMaxRunning,omitempty"`
	Jobs               []JobScheduleConfig `json:"jobs,omitempty"`
	DatabasePath       string              `json:"databasePath,omitempty"`
	EpgRetentionDays   int                 `json:"epgRetentionDays,omitempty"`
	EpgRetrievalTime   int                 `json:"epgRetrievalTime,omitempty"`
	EpgStaleAfter      int                 `json:"epgStaleAfter,omitempty"`
	LogoGatherDuration int                 `json:"logoGatherDuration,omitempty"`
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
	Insecure    bool                `json:"insecure,omitempty"`
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
		DatabasePath:       "./mahiron.db",
		EpgRetentionDays:   3,
		EpgRetrievalTime:   600000,
		EpgStaleAfter:      7200000,
		LogoGatherDuration: 86400000,
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
		config.Observability.ServiceName = "mahiron5"
	}

	switch config.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, errors.New("invalid log level")
	}
	if config.JobMaxRunning == 0 {
		config.JobMaxRunning = 1
	}
	if config.JobMaxRunning < 1 || config.JobMaxRunning > 100 {
		return nil, errors.New("jobMaxRunning must be between 1 and 100")
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
	if config.LogoGatherDuration <= 0 {
		return nil, errors.New("logoGatherDuration must be > 0")
	}

	return &config, nil
}
