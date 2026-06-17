package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type SystemConfig struct {
	Addresses []ServerAddress     `json:"addresses"`
	LogLevel  string              `json:"logLevel,omitempty"`
	Jobs      []JobScheduleConfig `json:"jobs,omitempty"`
}

type JobScheduleConfig struct {
	Key      string `json:"key"`
	Schedule string `json:"schedule"`
}

type ServerAddress struct {
	Http string `json:"http,omitempty"`
	Unix string `json:"unix,omitempty"`
}

func LoadAndParseSystemConfig(filePath string) (*SystemConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config SystemConfig
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

	switch config.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, errors.New("invalid log level")
	}

	return &config, nil
}
