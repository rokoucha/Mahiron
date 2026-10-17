package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type TunersConfig []*TunerConfig

type TunerConfig struct {
	// https://github.com/Chinachu/Mirakurun/blob/61c4155d2535c56fbf6fd379c5e8aba779fd642b/api.d.ts#L297
	Name              string            `json:"name"`
	Agent             *AgentTunerConfig `json:"agent,omitempty"`
	Types             []string          `json:"types,omitempty"`
	Command           string            `json:"command,omitempty"`
	DvbDevicePath     string            `json:"dvbDevicePath,omitempty"`
	Decoder           string            `json:"decoder,omitempty"`
	B61Decoder        string            `json:"b61Decoder,omitempty"`
	IsDisabled        bool              `json:"isDisabled,omitempty"`
	StartupRetryMax   int               `json:"startupRetryMax,omitempty"`
	StartupTimeout    int               `json:"startupTimeout,omitempty"`
	StartupRetryDelay int               `json:"startupRetryDelay,omitempty"`
}

type AgentTunerConfig struct {
	Decoder       string `json:"decoder,omitempty"`
	B61Decoder    string `json:"b61Decoder,omitempty"`
	Name          string `json:"name"`
	Command       string `json:"command"`
	DvbDevicePath string `json:"dvbDevicePath,omitempty"`
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
	seen := map[string]bool{}
	for _, tuner := range config {
		if tuner == nil {
			return nil, errors.New("tuner must not be null")
		}
		if tuner.Agent != nil {
			key := tuner.Agent.Name + "/" + tuner.Name
			if seen[key] {
				return nil, errors.New("duplicate agent tuner name")
			}
			seen[key] = true
		}
		if tuner.Name == "" {
			return nil, errors.New("tuner name is required")
		}
		if tuner.Agent != nil {
			if tuner.Agent.Name == "" || tuner.Agent.Command == "" {
				return nil, errors.New("agent name and command are required")
			}
			if (tuner.Agent.Decoder != "" && tuner.Decoder != "") || (tuner.Agent.B61Decoder != "" && tuner.B61Decoder != "") {
				return nil, errors.New("agent and local decoders for the same transport are mutually exclusive")
			}
			if tuner.Command != "" || tuner.DvbDevicePath != "" {
				return nil, errors.New("agent and local command/device are mutually exclusive")
			}
		} else if tuner.Command == "" {
			return nil, errors.New("command is required")
		}
		if tuner.DvbDevicePath != "" && tuner.Command == "" {
			return nil, errors.New("dvbDevicePath is only allowed when command is set")
		}
		if len(tuner.Types) == 0 {
			return nil, errors.New("at least one types is required")
		}
		if tuner.Agent != nil && (tuner.StartupRetryMax > 100 || tuner.StartupTimeout > 3600000 || tuner.StartupRetryDelay > 3600000) {
			return nil, errors.New("agent retry settings exceed limits")
		}
		if tuner.StartupRetryMax < 0 {
			return nil, errors.New("startupRetryMax must be >= 0")
		}
		if tuner.StartupTimeout < 0 {
			return nil, errors.New("startupTimeout must be >= 0")
		}
		if tuner.StartupRetryDelay < 0 {
			return nil, errors.New("startupRetryDelay must be >= 0")
		}
	}

	return config, nil
}
