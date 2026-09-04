package remote

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type remoteTuner struct {
	Index              int      `json:"index"`
	Name               string   `json:"name"`
	Types              []string `json:"types"`
	Command            string   `json:"command"`
	PID                int      `json:"pid"`
	IsAvailable        bool     `json:"isAvailable"`
	IsFree             bool     `json:"isFree"`
	IsUsing            bool     `json:"isUsing"`
	IsFault            bool     `json:"isFault"`
	CurrentChannelType string   `json:"currentChannelType"`
	CurrentChannel     string   `json:"currentChannel"`
	TunedChannelType   string   `json:"tunedChannelType"`
	TunedChannel       string   `json:"tunedChannel"`
}

func (t remoteTuner) Status() tuner.Status {
	return tuner.Status{
		Index: t.Index, Name: t.Name, Types: t.Types, Command: t.Command, PID: t.PID,
		IsAvailable: t.IsAvailable, IsFree: t.IsFree, IsUsing: t.IsUsing, IsFault: t.IsFault,
		CurrentChannelType: t.CurrentChannelType, CurrentChannel: t.CurrentChannel,
		TunedChannelType: t.TunedChannelType, TunedChannel: t.TunedChannel,
	}
}

func (t remoteTuner) matchesRoute(channelType, channel string) bool {
	if channel == "" {
		return false
	}
	return t.TunedChannelType == channelType && t.TunedChannel == channel ||
		t.CurrentChannelType == channelType && t.CurrentChannel == channel
}

type remoteService struct {
	ServiceID           uint16 `json:"serviceId"`
	NetworkID           uint16 `json:"networkId"`
	TransportStreamID   uint16 `json:"transportStreamId"`
	Name                string `json:"name"`
	Type                int    `json:"type"`
	EITScheduleFlag     *bool  `json:"eitScheduleFlag"`
	EITPresentFollowing *bool  `json:"eitPresentFollowing"`
	LogoID              *int64 `json:"logoId"`
	HasLogoData         bool   `json:"hasLogoData"`
	RemoteControlKeyID  int    `json:"remoteControlKeyId"`
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

func readRemoteEvents(ctx context.Context, src io.Reader, updater ProgramUpdater, updateTuner func(string, tuner.Status)) error {
	return readRemoteEventsBatched(ctx, src, updater, updateTuner, 250*time.Millisecond, 256)
}

type scannedRemoteEvent struct {
	line []byte
	err  error
}

// readRemoteEventsBatched keeps the event stream moving while amortizing the
// durable SQLite commit used by ProgramManager. A remote can emit tens of
// thousands of program updates per hour; committing each event separately is
// especially expensive when the database lives on network storage.
func readRemoteEventsBatched(ctx context.Context, src io.Reader, updater ProgramUpdater, updateTuner func(string, tuner.Status), flushInterval time.Duration, maxBatchSize int) error {
	if flushInterval <= 0 {
		flushInterval = 250 * time.Millisecond
	}
	if maxBatchSize <= 0 {
		maxBatchSize = 256
	}

	scanCtx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()
	scanned := make(chan scannedRemoteEvent, maxBatchSize)
	go scanRemoteEvents(scanCtx, src, scanned)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	pending := make(map[int64]*program.Program, maxBatchSize)
	order := make([]int64, 0, maxBatchSize)
	flush := func() error {
		if len(pending) == 0 || updater == nil {
			return nil
		}
		programs := make([]*program.Program, 0, len(pending))
		for _, id := range order {
			if item, ok := pending[id]; ok {
				programs = append(programs, item)
			}
		}
		if err := updater.UpsertPrograms(ctx, programs); err != nil {
			return err
		}
		clear(pending)
		order = order[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case item, ok := <-scanned:
			if !ok {
				return flush()
			}
			if item.err != nil {
				if errors.Is(item.err, context.Canceled) {
					return nil
				}
				return errors.Join(item.err, flush())
			}
			var event remoteEvent
			if json.Unmarshal(item.line, &event) != nil {
				continue
			}
			switch event.Resource {
			case "program":
				if updater == nil || event.Type != "update" && event.Type != "create" {
					continue
				}
				var item remoteProgram
				if json.Unmarshal(event.Data, &item) != nil {
					continue
				}
				program := item.Program()
				if _, exists := pending[program.ID]; !exists {
					order = append(order, program.ID)
				}
				pending[program.ID] = program
				if len(pending) >= maxBatchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			case "tuner":
				if updateTuner == nil {
					continue
				}
				var item remoteTuner
				if json.Unmarshal(event.Data, &item) == nil {
					updateTuner(event.Type, item.Status())
				}
			}
		}
	}
}

func scanRemoteEvents(ctx context.Context, src io.Reader, dst chan<- scannedRemoteEvent) {
	defer close(dst)
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSuffix(bytes.TrimSpace(scanner.Bytes()), []byte(","))
		if len(line) == 0 || bytes.Equal(line, []byte("[")) || bytes.Equal(line, []byte("]")) {
			continue
		}
		item := scannedRemoteEvent{line: bytes.Clone(line)}
		select {
		case dst <- item:
		case <-ctx.Done():
			return
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		select {
		case dst <- scannedRemoteEvent{err: err}:
		case <-ctx.Done():
		}
	}
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
