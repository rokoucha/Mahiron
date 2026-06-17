package job

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/stream"
	"github.com/21S1298001/Mahiron5/tuner"
)

const (
	EPGGathererKey  = "epg-gatherer"
	EPGGathererName = "EPG Gatherer"

	EPGGathererDefaultSchedule = "20,50 * * * *"
)

func RegisterEPGGatherer(mgr *JobManager, pm *program.ProgramManager, stm *stream.StreamManager, tm *tuner.TunerManager, channels config.ChannelsConfig) {
	mgr.Register(JobDefinition{
		Key:          EPGGathererKey,
		Name:         EPGGathererName,
		Handler:      epgGathererHandler(pm, stm, tm, channels),
		IsRerunnable: true,
	})
}

func epgGathererHandler(pm *program.ProgramManager, stm *stream.StreamManager, tm *tuner.TunerManager, channels config.ChannelsConfig) func(context.Context) error {
	return func(ctx context.Context) error {
		var collected, piggybacked, skipped, failed int

		for _, channel := range channels {
			select {
			case <-ctx.Done():
				slog.Info("EPG gatherer aborted", "collected", collected, "piggybacked", piggybacked, "skipped", skipped, "failed", failed)
				return ctx.Err()
			default:
			}

			if channel.IsDisabled != nil && *channel.IsDisabled {
				continue
			}

			group := channel.Type
			if len(channel.TunerGroups) > 0 {
				group = channel.TunerGroups[0]
			}

			if stm.HasSession(channel.Type, channel.Channel) {
				session, err := stm.GetOrCreate(ctx, channel.Type, channel.Channel)
				if err != nil {
					failed++
					slog.Error("failed to get stream session for EPG piggyback", "channel", channel.Channel, "err", err)
					continue
				}
				slog.Debug("starting EPG collection piggyback", "type", channel.Type, "channel", channel.Channel)
				if err := collectSessionEPG(ctx, pm, session); err != nil {
					failed++
					slog.Error("failed to collect EPG (piggyback)", "channel", channel.Channel, "err", err)
					continue
				}
				piggybacked++
				slog.Debug("finished EPG collection piggyback", "group", group, "type", channel.Type, "channel", channel.Channel)
				continue
			}

			if stm.ActiveSessionCountByGroup(group) >= tm.TunerCountByGroup(group) {
				skipped++
				slog.Info("skipping EPG collection: tuner unavailable", "group", group, "channel", channel.Channel)
				continue
			}

			session, err := stm.GetOrCreate(ctx, channel.Type, channel.Channel)
			if err != nil {
				failed++
				slog.Error("failed to create stream session for EPG", "channel", channel.Channel, "err", err)
				continue
			}
			slog.Debug("starting EPG collection", "type", channel.Type, "channel", channel.Channel)
			if err := collectSessionEPG(ctx, pm, session); err != nil {
				failed++
				slog.Error("failed to collect EPG", "channel", channel.Channel, "err", err)
				continue
			}
			collected++
			slog.Debug("finished EPG collection", "group", group, "type", channel.Type, "channel", channel.Channel)
		}

		slog.Info("EPG gatherer completed", "collected", collected, "piggybacked", piggybacked, "skipped", skipped, "failed", failed)
		return nil
	}
}

func collectSessionEPG(ctx context.Context, pm *program.ProgramManager, session *stream.ChannelSession) error {
	collectCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		slog.Debug("starting EITS collection")
		errCh <- collectEITSUntilComplete(collectCtx, pm, session.CollectEITS)
		slog.Debug("finished EITS collection")
		cancel()
	}()
	go func() {
		defer wg.Done()
		slog.Debug("starting EITPF collection")
		errCh <- collectEITJSONL(collectCtx, pm, session.CollectEITPF)
		slog.Debug("finished EITPF collection")
	}()
	wg.Wait()
	close(errCh)

	var result error
	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func collectEITSUntilComplete(ctx context.Context, pm *program.ProgramManager, collect func(context.Context, io.Writer) error) error {
	collectCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	r, w := io.Pipe()
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- readEITSUntilComplete(collectCtx, cancel, pm, r)
	}()

	collectErr := collect(collectCtx, w)
	_ = w.Close()
	readErr := <-readErrCh
	_ = r.Close()
	if errors.Is(collectErr, context.Canceled) && readErr == nil {
		collectErr = nil
	}
	return errors.Join(collectErr, readErr)
}

func collectEITJSONL(ctx context.Context, pm *program.ProgramManager, collect func(context.Context, io.Writer) error) error {
	r, w := io.Pipe()
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- pm.ReadEITJSONL(ctx, r)
	}()

	collectErr := collect(ctx, w)
	_ = w.Close()
	readErr := <-readErrCh
	_ = r.Close()
	return errors.Join(collectErr, readErr)
}

func readEITSUntilComplete(ctx context.Context, cancel context.CancelFunc, pm *program.ProgramManager, r io.Reader) error {
	tracker := program.NewEITSCompletionTracker()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var section program.EITSection
		if err := json.Unmarshal(line, &section); err != nil {
			return err
		}
		if err := pm.UpsertEITSection(&section); err != nil {
			return err
		}
		complete := tracker.Observe(&section)
		collectedSections, totalSections, _ := tracker.Progress(&section)
		slog.Debug("received EITS section",
			"networkId", section.OriginalNetworkID,
			"transportStreamId", section.TransportStreamID,
			"serviceId", section.ServiceID,
			"tableId", section.TableID,
			"versionNumber", section.VersionNumber,
			"sectionNumber", section.SectionNumber,
			"lastSectionNumber", section.LastSectionNumber,
			"collectedSections", collectedSections,
			"totalSections", totalSections,
			"events", len(section.Events),
		)
		if complete {
			slog.Debug("completed EITS collection", "tables", tracker.TableCount())
			cancel()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if tracker.Complete() {
		return nil
	}
	if ctx.Err() == nil {
		return errors.New("EITS stream ended before all sections were collected")
	}
	return ctx.Err()
}
