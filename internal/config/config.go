package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const DefaultConfigDir = "./config"

type Config struct {
	Channels ChannelsConfig
	Remotes  RemotesConfig
	System   *SystemConfig
	Tuners   TunersConfig
}

func LoadAndParseConfig() (*Config, error) {
	return LoadAndParseConfigFromDir(DefaultConfigDir)
}

func LoadAndParseConfigFromDir(configDir string) (*Config, error) {
	channels, err := LoadAndParseChannelsConfig(filepath.Join(configDir, "channels.yml"))
	if err != nil {
		return nil, err
	}

	system, err := LoadAndParseSystemConfig(filepath.Join(configDir, "server.yml"))
	if err != nil {
		return nil, err
	}

	tuners, err := LoadAndParseTunersConfig(filepath.Join(configDir, "tuners.yml"))
	if err != nil {
		return nil, err
	}

	knownAgents := map[string]bool{}
	for _, a := range system.Agents {
		knownAgents[a.Name] = true
	}
	devices := map[string]bool{}
	for _, t := range tuners {
		if t.Agent != nil {
			if !knownAgents[t.Agent.Name] {
				return nil, fmt.Errorf("tuner %s references unknown agent %s", t.Name, t.Agent.Name)
			}
			if t.Agent.DvbDevicePath != "" {
				key := t.Agent.Name + "/" + t.Agent.DvbDevicePath
				if devices[key] {
					return nil, fmt.Errorf("duplicate agent DVB device for %s", t.Name)
				}
				devices[key] = true
			}
		}
	}

	remotes, err := loadRemotesForChannels(filepath.Join(configDir, "remotes.yml"), channels)
	if err != nil {
		return nil, err
	}

	return &Config{
		Channels: channels,
		Remotes:  remotes,
		System:   system,
		Tuners:   tuners,
	}, nil
}

func loadRemotesForChannels(filePath string, channels ChannelsConfig) (RemotesConfig, error) {
	needsRemotes := false
	for _, channel := range channels {
		for _, route := range channel.RoutesOrDefault() {
			if route.Remote != "" {
				needsRemotes = true
				break
			}
		}
	}

	remotes, err := LoadAndParseRemotesConfig(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !needsRemotes {
			return nil, nil
		}
		return nil, err
	}

	for _, channel := range channels {
		for _, route := range channel.RoutesOrDefault() {
			if route.Remote == "" {
				continue
			}
			if remotes.Get(route.Remote) == nil {
				return nil, fmt.Errorf("remote %q is not configured", route.Remote)
			}
		}
	}

	return remotes, nil
}
