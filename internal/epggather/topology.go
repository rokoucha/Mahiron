package epggather

import (
	"context"
	"fmt"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

type Candidate struct {
	Type    string
	Channel string
}

type Network struct {
	Candidates []Candidate
	Services   []model.ServiceKey
}

// groupServicesByNetwork lists each network's services and the channels to
// gather them from. networkWideEIT reports whether every stream of a network
// carries the whole network's EIT, in which case any channel of the type
// serves the network.
func groupServicesByNetwork(services []*service.Service, channels config.ChannelsConfig, networkWideEIT func(uint16) bool) map[uint16]*Network {
	byChannel := make(map[string][]uint16)
	networkTypes := make(map[uint16]map[string]bool)
	typeNetworks := make(map[string]map[uint16]bool)
	for _, item := range services {
		key := epgChannelKey(item.ChannelType, item.ChannelId)
		byChannel[key] = append(byChannel[key], item.Key.NetworkID)
		if networkTypes[item.Key.NetworkID] == nil {
			networkTypes[item.Key.NetworkID] = make(map[string]bool)
		}
		networkTypes[item.Key.NetworkID][item.ChannelType] = true
		if typeNetworks[item.ChannelType] == nil {
			typeNetworks[item.ChannelType] = make(map[uint16]bool)
		}
		typeNetworks[item.ChannelType][item.Key.NetworkID] = true
	}
	groups := make(map[uint16]*Network)
	seen := make(map[uint16]map[string]bool)
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := epgChannelKey(configured.Type, configured.Channel)
		candidateNetworks := byChannel[key]
		if broadNetwork, ok := broadEPGCandidateNetwork(configured.Type, typeNetworks, networkWideEIT); ok {
			candidateNetworks = []uint16{broadNetwork}
		}
		for _, nid := range candidateNetworks {
			if groups[nid] == nil {
				groups[nid] = &Network{}
			}
			if seen[nid] == nil {
				seen[nid] = make(map[string]bool)
			}
			if seen[nid][key] {
				continue
			}
			seen[nid][key] = true
			groups[nid].Candidates = append(groups[nid].Candidates, Candidate{Type: configured.Type, Channel: configured.Channel})
		}
	}
	serviceSeen := make(map[model.ServiceKey]bool)
	for _, svc := range services {
		if !svc.EITSchedule {
			continue
		}
		key := svc.Key
		if groups[svc.Key.NetworkID] != nil && !serviceSeen[key] {
			groups[svc.Key.NetworkID].Services = append(groups[svc.Key.NetworkID].Services, key)
			serviceSeen[key] = true
		}
	}
	return groups
}

func buildNetworkInputs(ctx context.Context, serviceStore ServiceStore, channels config.ChannelsConfig, networkID uint16, networkWideEIT func(uint16) bool) ([]Candidate, []model.ServiceKey, error) {
	storedServices, err := serviceStore.GetServices(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get services: %w", err)
	}
	byChannel := make(map[string]bool)
	networkTypes := make(map[string]bool)
	typeNetworks := make(map[string]map[uint16]bool)
	for _, item := range storedServices {
		if typeNetworks[item.ChannelType] == nil {
			typeNetworks[item.ChannelType] = make(map[uint16]bool)
		}
		typeNetworks[item.ChannelType][item.Key.NetworkID] = true
		if item.Key.NetworkID != networkID {
			continue
		}
		key := epgChannelKey(item.ChannelType, item.ChannelId)
		byChannel[key] = true
		networkTypes[item.ChannelType] = true
	}
	var candidates []Candidate
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := epgChannelKey(configured.Type, configured.Channel)
		if byChannel[key] || broadEPGCandidateForNetwork(configured.Type, typeNetworks, networkID, networkWideEIT) && networkTypes[configured.Type] {
			candidates = append(candidates, Candidate{Type: configured.Type, Channel: configured.Channel})
		}
	}
	serviceSeen := make(map[model.ServiceKey]bool)
	var networkServices []model.ServiceKey
	for _, svc := range storedServices {
		if svc.Key.NetworkID != networkID {
			continue
		}
		if !svc.EITSchedule {
			continue
		}
		key := svc.Key
		if !serviceSeen[key] {
			serviceSeen[key] = true
			networkServices = append(networkServices, key)
		}
	}
	return candidates, networkServices, nil
}

func broadEPGCandidateForNetwork(channelType string, typeNetworks map[string]map[uint16]bool, networkID uint16, networkWideEIT func(uint16) bool) bool {
	return networkWideEIT(networkID) && len(typeNetworks[channelType]) == 1 && typeNetworks[channelType][networkID]
}

func broadEPGCandidateNetwork(channelType string, typeNetworks map[string]map[uint16]bool, networkWideEIT func(uint16) bool) (uint16, bool) {
	if len(typeNetworks[channelType]) != 1 {
		return 0, false
	}
	for networkID := range typeNetworks[channelType] {
		return networkID, networkWideEIT(networkID)
	}
	return 0, false
}

func epgChannelKey(channelType, channelID string) string {
	return channelType + "\x00" + channelID
}
