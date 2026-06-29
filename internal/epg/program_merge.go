package epg

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
	EventID   uint16
	StartAt   int64
	Duration  int
}

func fillProgramsFromSharedPeers(programs []*program.Program) {
	peers := make(map[programPeerKey][]*program.Program)
	for _, item := range programs {
		if item == nil {
			continue
		}
		key := programPeerKey{
			NetworkID: item.NetworkID,
			EventID:   item.EventID,
			StartAt:   item.StartAt,
			Duration:  item.Duration,
		}
		peers[key] = append(peers[key], item)
	}
	for _, group := range peers {
		for _, item := range group {
			fillProgramFromPeers(item, group)
		}
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
