package epggather

import (
	"fmt"

	"github.com/21S1298001/mahiron/internal/program"
)

const (
	lowQualityMinimumPrograms     = 10
	lowQualityMissingTitlePercent = 80
)

type programPeerKey struct {
	NetworkID uint16
	ServiceID uint16
	EventID   uint16
}

func fillProgramsFromSharedPeers(programs []*program.Program) {
	parent := make(map[programPeerKey]programPeerKey)

	var find func(programPeerKey) programPeerKey
	find = func(key programPeerKey) programPeerKey {
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

	union := func(a, b programPeerKey) {
		rootA := find(a)
		rootB := find(b)
		if rootA == rootB {
			return
		}
		parent[rootB] = rootA
	}

	for _, item := range programs {
		if item == nil {
			continue
		}
		source := programKey(item)
		find(source)
		for _, related := range item.RelatedItems {
			if related.Type != program.RelatedItemTypeShared || related.ServiceID == 0 || related.EventID == 0 {
				continue
			}
			networkID := item.NetworkID
			if related.NetworkID != nil {
				networkID = *related.NetworkID
			}
			union(source, programPeerKey{
				NetworkID: networkID,
				ServiceID: related.ServiceID,
				EventID:   related.EventID,
			})
		}
	}

	peers := make(map[programPeerKey][]*program.Program)
	for _, item := range programs {
		if item == nil {
			continue
		}
		key := programKey(item)
		root := find(key)
		peers[root] = append(peers[root], item)
	}

	for _, group := range peers {
		for _, item := range group {
			fillProgramFromPeers(item, group)
		}
	}
}

func programKey(item *program.Program) programPeerKey {
	return programPeerKey{
		NetworkID: item.NetworkID,
		ServiceID: item.ServiceID,
		EventID:   item.EventID,
	}
}

func fillProgramFromPeers(item *program.Program, peers []*program.Program) {
	if item == nil {
		return
	}
	for _, peer := range peers {
		if peer == nil || peer == item {
			continue
		}
		if item.Name == "" && peer.Name != "" {
			item.Name = peer.Name
		}
		if item.Description == "" && peer.Description != "" {
			item.Description = peer.Description
		}
		if len(item.Genres) == 0 && len(peer.Genres) > 0 {
			item.Genres = append([]program.Genre(nil), peer.Genres...)
		}
		if item.Video == nil && peer.Video != nil {
			video := *peer.Video
			item.Video = &video
		}
		if len(item.Audios) == 0 && len(peer.Audios) > 0 {
			item.Audios = append([]program.Audio(nil), peer.Audios...)
		}
		if len(item.Extended) == 0 && len(peer.Extended) > 0 {
			item.Extended = cloneStringMap(peer.Extended)
		}
		if item.Series == nil && peer.Series != nil {
			series := *peer.Series
			item.Series = &series
		}
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func lowQualityProgramWarning(programs []*program.Program) string {
	missingTitle, total := programTitleCounts(programs)
	if total < lowQualityMinimumPrograms || missingTitle*100 < total*lowQualityMissingTitlePercent {
		return ""
	}
	return fmt.Sprintf("low quality EITS: %d/%d programs missing titles", missingTitle, total)
}

func programTitleCounts(programs []*program.Program) (int, int) {
	missingTitle := 0
	total := 0
	for _, item := range programs {
		if item == nil {
			continue
		}
		total++
		if item.Name == "" {
			missingTitle++
		}
	}
	return missingTitle, total
}
