package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type ChannelsConfig []ChannelConfig

type ChannelConfig struct {
	// https://github.com/Chinachu/Mirakurun/blob/61c4155d2535c56fbf6fd379c5e8aba779fd642b/api.d.ts#L320
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Channel     string         `json:"channel"`
	ServiceId   *uint32        `json:"serviceId,omitempty"`
	TsmfRelTs   *uint8         `json:"tsmfRelTs,omitempty"`
	CommandVars map[string]any `json:"commandVars,omitempty"`
	IsDisabled  *bool          `json:"isDisabled,omitempty"`
	Satelite    *string        `json:"satelite,omitempty"`  // deprecated
	Satellite   *string        `json:"satellite,omitempty"` // deprecated
	Space       *uint8         `json:"space,omitempty"`     // deprecated
	Freq        *uint32        `json:"freq,omitempty"`      // deprecated
	Polarity    *string        `json:"polarity,omitempty"`  // deprecated

	// Mahiron extension
	Routes                []ChannelRouteConfig `json:"routes,omitempty"`
	DeprecatedTunerGroups []string             `json:"tunerGroups,omitempty"`
}

type ChannelRouteConfig struct {
	Id          string         `json:"id,omitempty"`
	Type        string         `json:"type"`
	Channel     string         `json:"channel"`
	ServiceId   *uint32        `json:"serviceId,omitempty"`
	TsmfRelTs   *uint8         `json:"tsmfRelTs,omitempty"`
	CommandVars map[string]any `json:"commandVars,omitempty"`
	IsDisabled  *bool          `json:"isDisabled,omitempty"`
	Priority    *int           `json:"priority,omitempty"`
}

func LoadAndParseChannelsConfig(filePath string) (ChannelsConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config ChannelsConfig
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}
	if len(config) == 0 {
		return nil, errors.New("at least one channel is required")
	}

	no := false

	for i, channel := range config {
		if channel.Name == "" {
			return nil, errors.New("channel name is required")
		}
		if channel.Type == "" {
			return nil, errors.New("channel type is required")
		}
		if channel.Channel == "" {
			return nil, errors.New("channel symbol is required")
		}
		if len(channel.DeprecatedTunerGroups) > 0 {
			return nil, errors.New("tunerGroups is no longer supported; use routes instead")
		}
		if channel.TsmfRelTs != nil && channel.ServiceId == nil {
			return nil, errors.New("serviceId is required when tsmfRelTs is set")
		}
		if channel.TsmfRelTs != nil && *channel.TsmfRelTs > 0x0F {
			return nil, errors.New("tsmfRelTs must be between 0 and 15")
		}
		if channel.CommandVars != nil && (channel.Satelite != nil ||
			channel.Satellite != nil ||
			channel.Space != nil ||
			channel.Freq != nil ||
			channel.Polarity != nil) {
			return nil, errors.New("commandVars cannot be used with satelite, satellite, space, freq, or polarity")
		}

		if channel.IsDisabled == nil {
			config[i].IsDisabled = &no
		}
		if channel.CommandVars == nil {
			config[i].CommandVars = make(map[string]any)
		}
		if channel.Satelite != nil {
			config[i].CommandVars["satellite"] = *channel.Satelite
			config[i].Satelite = nil
		}
		if channel.Satellite != nil {
			config[i].CommandVars["satellite"] = *channel.Satellite
			config[i].Satellite = nil
		}
		if channel.Space != nil {
			config[i].CommandVars["space"] = *channel.Space
			config[i].Space = nil
		}
		if channel.Freq != nil {
			config[i].CommandVars["freq"] = *channel.Freq
			config[i].Freq = nil
		}
		if channel.Polarity != nil {
			config[i].CommandVars["polarity"] = *channel.Polarity
			config[i].Polarity = nil
		}
		routes, err := normalizeRoutes(config[i])
		if err != nil {
			return nil, err
		}
		config[i].Routes = routes
	}

	return config, nil
}

func normalizeRoutes(channel ChannelConfig) ([]ChannelRouteConfig, error) {
	no := false
	routes := channel.Routes
	if len(routes) == 0 {
		routes = []ChannelRouteConfig{
			{
				Id:          "default",
				Type:        channel.Type,
				Channel:     channel.Channel,
				ServiceId:   channel.ServiceId,
				TsmfRelTs:   channel.TsmfRelTs,
				CommandVars: channel.CommandVars,
				IsDisabled:  channel.IsDisabled,
			},
		}
	}

	seen := make(map[string]struct{}, len(routes))
	for i := range routes {
		if routes[i].Type == "" {
			return nil, errors.New("route type is required")
		}
		if routes[i].Channel == "" {
			return nil, errors.New("route channel is required")
		}
		if routes[i].Id == "" {
			routes[i].Id = routes[i].Type + ":" + routes[i].Channel
		}
		if _, ok := seen[routes[i].Id]; ok {
			return nil, errors.New("duplicate route id")
		}
		seen[routes[i].Id] = struct{}{}
		if routes[i].TsmfRelTs != nil && routes[i].ServiceId == nil {
			return nil, errors.New("serviceId is required when route tsmfRelTs is set")
		}
		if routes[i].TsmfRelTs != nil && *routes[i].TsmfRelTs > 0x0F {
			return nil, errors.New("route tsmfRelTs must be between 0 and 15")
		}
		if routes[i].CommandVars == nil {
			routes[i].CommandVars = make(map[string]any)
		}
		if routes[i].IsDisabled == nil {
			routes[i].IsDisabled = &no
		}
	}
	return routes, nil
}

func (c ChannelConfig) RouteChannelConfig(route ChannelRouteConfig) ChannelConfig {
	routeChannel := c
	routeChannel.Type = route.Type
	routeChannel.Channel = route.Channel
	routeChannel.ServiceId = route.ServiceId
	routeChannel.TsmfRelTs = route.TsmfRelTs
	routeChannel.CommandVars = route.CommandVars
	routeChannel.IsDisabled = route.IsDisabled
	routeChannel.Routes = nil
	routeChannel.DeprecatedTunerGroups = nil
	return routeChannel
}

func (c ChannelConfig) RoutesOrDefault() []ChannelRouteConfig {
	if len(c.Routes) > 0 {
		return c.Routes
	}
	return []ChannelRouteConfig{
		{
			Id:          "default",
			Type:        c.Type,
			Channel:     c.Channel,
			ServiceId:   c.ServiceId,
			TsmfRelTs:   c.TsmfRelTs,
			CommandVars: c.CommandVars,
			IsDisabled:  c.IsDisabled,
		},
	}
}
