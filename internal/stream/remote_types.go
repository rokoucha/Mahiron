package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"

	"github.com/21S1298001/mahiron/internal/program"
)

type remoteTuner struct {
	Types              []string `json:"types"`
	IsAvailable        bool     `json:"isAvailable"`
	IsFree             bool     `json:"isFree"`
	IsFault            bool     `json:"isFault"`
	CurrentChannelType string   `json:"currentChannelType"`
	CurrentChannel     string   `json:"currentChannel"`
	TunedChannelType   string   `json:"tunedChannelType"`
	TunedChannel       string   `json:"tunedChannel"`
}

func (t remoteTuner) matchesRoute(channelType, channel string) bool {
	if channel == "" {
		return false
	}
	return t.TunedChannelType == channelType && t.TunedChannel == channel ||
		t.CurrentChannelType == channelType && t.CurrentChannel == channel
}

type remoteService struct {
	ServiceID          uint16 `json:"serviceId"`
	NetworkID          uint16 `json:"networkId"`
	TransportStreamID  uint16 `json:"transportStreamId"`
	Name               string `json:"name"`
	Type               int    `json:"type"`
	LogoID             uint64 `json:"logoId"`
	RemoteControlKeyID int    `json:"remoteControlKeyId"`
}

func uint8Ptr(v uint8) *uint8 { return &v }

type remoteProgram struct {
	ID           int64               `json:"id"`
	EventID      uint16              `json:"eventId"`
	ServiceID    uint16              `json:"serviceId"`
	NetworkID    uint16              `json:"networkId"`
	StartAt      int64               `json:"startAt"`
	Duration     int                 `json:"duration"`
	IsFree       bool                `json:"isFree"`
	Name         string              `json:"name"`
	Description  string              `json:"description"`
	Genres       []remoteGenre       `json:"genres"`
	Video        *remoteVideo        `json:"video"`
	Audios       []remoteAudio       `json:"audios"`
	Extended     map[string]string   `json:"extended"`
	RelatedItems []remoteRelatedItem `json:"relatedItems"`
	Series       *remoteSeries       `json:"series"`
}

type remoteEvent struct {
	Resource string          `json:"resource"`
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
}

func readRemoteProgramEvents(ctx context.Context, src io.Reader, updater ProgramUpdater) error {
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || bytes.Equal(line, []byte("[")) || bytes.Equal(line, []byte(",")) || bytes.Equal(line, []byte("]")) {
			continue
		}
		line = bytes.TrimSuffix(line, []byte(","))
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event remoteEvent
		if err := json.Unmarshal(line, &event); err != nil {
			slog.Debug("failed to decode remote program event", "err", err)
			continue
		}
		if event.Resource != "program" || event.Type != "update" && event.Type != "create" {
			continue
		}
		var remote remoteProgram
		if err := json.Unmarshal(event.Data, &remote); err != nil {
			slog.Debug("failed to decode remote program event data", "err", err)
			continue
		}
		if err := updater.UpsertPrograms(ctx, []*program.Program{remote.Program()}); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}

func (p remoteProgram) Program() *program.Program {
	prog := &program.Program{
		ID:           p.ID,
		EventID:      p.EventID,
		ServiceID:    p.ServiceID,
		NetworkID:    p.NetworkID,
		StartAt:      p.StartAt,
		Duration:     p.Duration,
		IsFree:       p.IsFree,
		Name:         p.Name,
		Description:  p.Description,
		Genres:       remoteGenres(p.Genres),
		Audios:       remoteAudios(p.Audios),
		Extended:     p.Extended,
		RelatedItems: remoteRelatedItems(p.RelatedItems),
	}
	if p.Video != nil {
		prog.Video = &program.Video{
			StreamContent: p.Video.StreamContent,
			ComponentType: p.Video.ComponentType,
		}
	}
	if p.Series != nil {
		pattern := -1
		if p.Series.Pattern != nil {
			pattern = *p.Series.Pattern
		}
		prog.Series = &program.Series{
			ID:          p.Series.ID,
			Repeat:      p.Series.Repeat,
			Pattern:     pattern,
			ExpiresAt:   p.Series.ExpiresAt,
			Episode:     p.Series.Episode,
			LastEpisode: p.Series.LastEpisode,
			Name:        p.Series.Name,
		}
	}
	return prog
}

type remoteGenre struct {
	Lv1 int `json:"lv1"`
	Lv2 int `json:"lv2"`
	Un1 int `json:"un1"`
	Un2 int `json:"un2"`
}

func remoteGenres(items []remoteGenre) []program.Genre {
	result := make([]program.Genre, len(items))
	for i, item := range items {
		result[i] = program.Genre{Lv1: item.Lv1, Lv2: item.Lv2, Un1: item.Un1, Un2: item.Un2}
	}
	return result
}

type remoteVideo struct {
	StreamContent int `json:"streamContent"`
	ComponentType int `json:"componentType"`
}

type remoteAudio struct {
	ComponentType int      `json:"componentType"`
	ComponentTag  *int     `json:"componentTag"`
	IsMain        *bool    `json:"isMain"`
	SamplingRate  *int     `json:"samplingRate"`
	Langs         []string `json:"langs"`
}

func remoteAudios(items []remoteAudio) []program.Audio {
	result := make([]program.Audio, len(items))
	for i, item := range items {
		result[i] = program.Audio{
			ComponentType: item.ComponentType,
			ComponentTag:  item.ComponentTag,
			IsMain:        item.IsMain,
			SamplingRate:  item.SamplingRate,
			Langs:         item.Langs,
		}
	}
	return result
}

type remoteRelatedItem struct {
	Type      string  `json:"type"`
	NetworkID *uint16 `json:"networkId"`
	ServiceID uint16  `json:"serviceId"`
	EventID   uint16  `json:"eventId"`
}

func remoteRelatedItems(items []remoteRelatedItem) []program.RelatedItem {
	result := make([]program.RelatedItem, len(items))
	for i, item := range items {
		result[i] = program.RelatedItem{
			Type:      program.RelatedItemType(item.Type),
			NetworkID: item.NetworkID,
			ServiceID: item.ServiceID,
			EventID:   item.EventID,
		}
	}
	return result
}

type remoteSeries struct {
	ID          int    `json:"id"`
	Repeat      int    `json:"repeat"`
	Pattern     *int   `json:"pattern"`
	ExpiresAt   *int64 `json:"expiresAt"`
	Episode     int    `json:"episode"`
	LastEpisode int    `json:"lastEpisode"`
	Name        string `json:"name"`
}
