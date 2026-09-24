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

	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
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
		remote, err := decodeRemoteProgram(event.Data)
		if err != nil {
			slog.Debug("failed to decode remote program event data", "err", err)
			continue
		}
		if err := updater.UpsertPrograms(ctx, []*program.Program{remote}); err != nil {
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
	return readRemoteEventsBatched(ctx, src, updater, updateTuner, 3*time.Second, 1000)
}

type scannedRemoteEvent struct {
	line []byte
	err  error
}

// readRemoteEventsBatched keeps the event stream moving while amortizing the
// durable SQLite commit used by program.Manager. A remote can emit tens of
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
				item, err := decodeRemoteProgram(event.Data)
				if err != nil {
					continue
				}
				program := item
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

// decodeRemoteProgram decodes a Mirakurun-compatible program with the ogen
// types and converts it through the shared conversion, the same one the API
// serves. Payloads missing required fields are rejected.
func decodeRemoteProgram(data json.RawMessage) (*program.Program, error) {
	var api apigen.Program
	if err := json.Unmarshal(data, &api); err != nil {
		return nil, err
	}
	return mirakurun.ProgramFromAPI(&api), nil
}
