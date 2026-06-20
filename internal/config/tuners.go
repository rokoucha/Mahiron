package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type TunersConfig []*TunerConfig

type TunerConfig struct {
	// https://github.com/Chinachu/Mirakurun/blob/61c4155d2535c56fbf6fd379c5e8aba779fd642b/api.d.ts#L297
	Name          string   `json:"name"`
	Types         []string `json:"types,omitempty"`
	Command       string   `json:"command,omitempty"`
	DvbDevicePath string   `json:"dvbDevicePath,omitempty"`
	Decoder       string   `json:"decoder,omitempty"`
	IsDisabled    bool     `json:"isDisabled,omitempty"`

	// Mahiron extension
	Remote *Remote `json:"remote,omitempty"`
}

type Remote struct {
	Url   string            `json:"url"`
	Types map[string]string `json:"types,omitempty"`
}

func LoadAndParseTunersConfig(filePath string) (TunersConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config TunersConfig
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}

	if len(config) == 0 {
		return nil, errors.New("at least one tuner is required")
	}
	for _, tuner := range config {
		if tuner.Name == "" {
			return nil, errors.New("tuner name is required")
		}
		if tuner.Command == "" && tuner.Remote == nil {
			return nil, errors.New("command or remote is required")
		}
		if tuner.Command != "" && tuner.Remote != nil {
			return nil, errors.New("only one command or remote is allowed")
		}
		if tuner.DvbDevicePath != "" && tuner.Command == "" {
			return nil, errors.New("dvbDevicePath is only allowed when command is set")
		}
		if len(tuner.Types) == 0 {
			return nil, errors.New("at least one types is required")
		}
		if tuner.Remote != nil && tuner.Remote.Url == "" {
			return nil, errors.New("remote url is required")
		}
	}

	return config, nil
}
