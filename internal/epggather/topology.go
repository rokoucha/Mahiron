package epggather

import (
	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

type Candidate struct {
	Type    string
	Channel string
}

// Stored groups API-backed EPG by network; live reception is scoped to a stream.
// It is separate from StreamID because zero is also a possible stream ID.
type GroupKey struct {
	NetworkID uint16
	StreamID  uint16
	Stored    bool
	// Channel distinguishes live inputs until a scan learns their stream ID.
	Channel Candidate
}

type Group struct {
	Candidates []Candidate
	Services   []model.ServiceKey
	// Zero means at least one service has never been gathered.
	OldestSuccessAt int64
}

func groupServices(services []*service.Service, channels config.ChannelsConfig, storedEPG func(string, string) bool) map[GroupKey]*Group {
	enabled := make(map[Candidate]bool)
	for _, ch := range channels {
		if !config.IsChannelDisabled(ch) {
			enabled[Candidate{Type: ch.Type, Channel: ch.Channel}] = true
		}
	}
	// Prefer stored EPG when the same service is reachable through both paths.
	stored := make(map[model.ServiceKey]bool)
	for _, svc := range services {
		c := Candidate{Type: svc.ChannelType, Channel: svc.ChannelId}
		if enabled[c] && storedEPG(c.Type, c.Channel) {
			stored[svc.Key] = true
		}
	}
	groups := make(map[GroupKey]*Group)
	candidates := make(map[GroupKey]map[Candidate]bool)
	seen := make(map[GroupKey]map[model.ServiceKey]bool)
	for _, svc := range services {
		c := Candidate{Type: svc.ChannelType, Channel: svc.ChannelId}
		if !svc.EITSchedule || !enabled[c] {
			continue
		}
		isStored := storedEPG(c.Type, c.Channel)
		if stored[svc.Key] != isStored {
			continue
		}
		key := GroupKey{NetworkID: svc.Key.NetworkID, StreamID: svc.Key.StreamID, Stored: isStored}
		if isStored {
			key.StreamID = 0
		} else if key.StreamID == 0 {
			key.Channel = c
		}
		group := groups[key]
		if group == nil {
			group = &Group{}
			groups[key] = group
			candidates[key] = make(map[Candidate]bool)
			seen[key] = make(map[model.ServiceKey]bool)
		}
		candidates[key][c] = true
		if seen[key][svc.Key] {
			continue
		}
		seen[key][svc.Key] = true
		success := int64(0)
		if svc.EPG.LastSuccessAt != nil {
			success = *svc.EPG.LastSuccessAt
		}
		if len(group.Services) == 0 || success < group.OldestSuccessAt {
			group.OldestSuccessAt = success
		}
		group.Services = append(group.Services, svc.Key)
	}
	// Keep configured order for deterministic candidate fallback.
	for _, ch := range channels {
		c := Candidate{Type: ch.Type, Channel: ch.Channel}
		for key, group := range groups {
			if candidates[key][c] {
				group.Candidates = append(group.Candidates, c)
				delete(candidates[key], c)
			}
		}
	}
	return groups
}
