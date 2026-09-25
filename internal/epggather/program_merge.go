package epggather

import (
	"fmt"

	"github.com/21S1298001/mahiron/internal/model"
)

const (
	lowQualityMinimumPrograms     = 10
	lowQualityMissingTitlePercent = 80
)

type eventPeerKey struct {
	NetworkID uint16
	ServiceID uint16
	EventID   uint16
}

// fillEventsFromSharedPeers fills what an event lacks from the events it
// shares with through event group type 1 (event sharing), across all the
// given groups. The events are modified in place.
func fillEventsFromSharedPeers(groups ...[]model.Event) {
	parent := make(map[eventPeerKey]eventPeerKey)

	var find func(eventPeerKey) eventPeerKey
	find = func(key eventPeerKey) eventPeerKey {
		current, ok := parent[key]
		if !ok {
			parent[key] = key
			return key
		}
		if current == key {
			return key
		}
		root := find(current)
		parent[key] = root
		return root
	}

	union := func(a, b eventPeerKey) {
		rootA := find(a)
		rootB := find(b)
		if rootA == rootB {
			return
		}
		parent[rootB] = rootA
	}

	for _, events := range groups {
		for i := range events {
			item := &events[i]
			source := eventKey(item)
			find(source)
			for _, related := range item.Related {
				if related.GroupType != model.EventGroupShared || related.ServiceID == 0 || related.EventID == 0 {
					continue
				}
				networkID := item.Key.NetworkID
				if related.NetworkID != 0 {
					networkID = related.NetworkID
				}
				union(source, eventPeerKey{
					NetworkID: networkID,
					ServiceID: related.ServiceID,
					EventID:   related.EventID,
				})
			}
		}
	}

	peers := make(map[eventPeerKey][]*model.Event)
	for _, events := range groups {
		for i := range events {
			item := &events[i]
			root := find(eventKey(item))
			peers[root] = append(peers[root], item)
		}
	}

	for _, group := range peers {
		for _, item := range group {
			fillEventFromPeers(item, group)
		}
	}
}

func eventKey(item *model.Event) eventPeerKey {
	return eventPeerKey{
		NetworkID: item.Key.NetworkID,
		ServiceID: item.Key.ServiceID,
		EventID:   item.EventID,
	}
}

func fillEventFromPeers(item *model.Event, peers []*model.Event) {
	for _, peer := range peers {
		if peer == item {
			continue
		}
		if item.Name == "" && peer.Name != "" {
			item.Name = peer.Name
		}
		if item.Description == "" && peer.Description != "" {
			item.Description = peer.Description
		}
		if len(item.Genres) == 0 && len(peer.Genres) > 0 {
			item.Genres = peer.Genres
		}
		if len(item.Videos) == 0 && len(peer.Videos) > 0 {
			item.Videos = peer.Videos
		}
		if len(item.Audios) == 0 && len(peer.Audios) > 0 {
			item.Audios = peer.Audios
		}
		if len(item.Extended) == 0 && len(peer.Extended) > 0 {
			item.Extended = peer.Extended
		}
		if item.Series == nil && peer.Series != nil {
			item.Series = peer.Series
		}
	}
}

func lowQualityEventWarning(events []model.Event) string {
	missingTitle, total := eventTitleCounts(events)
	if total < lowQualityMinimumPrograms || missingTitle*100 < total*lowQualityMissingTitlePercent {
		return ""
	}
	return fmt.Sprintf("low quality EITS: %d/%d programs missing titles", missingTitle, total)
}

func eventTitleCounts(events []model.Event) (int, int) {
	missingTitle := 0
	for _, item := range events {
		if item.Name == "" {
			missingTitle++
		}
	}
	return missingTitle, len(events)
}
