package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

var (
	ErrInvalidConfig = errors.New("invalid config")
)

type ServerConfig struct {
	Addresses []ServerAddress `json:"addresses"`
	LogLevel  string          `json:"logLevel"`
}

type ServerAddress struct {
	Http string `json:"http"`
	Unix string `json:"unix"`
}

func LoadAndParseServerConfig(filePath string) (*ServerConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config ServerConfig
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
			return nil, ErrInvalidConfig
		}
		if addr.Http != "" && addr.Unix != "" {
			return nil, ErrInvalidConfig
		}
	}

	if config.LogLevel == "" {
		config.LogLevel = "info"
	}

	switch config.LogLevel {
	case "debug", "info", "warn", "error":
		// valid log level
	default:
		return nil, ErrInvalidConfig
	}

	return &config, nil
}
