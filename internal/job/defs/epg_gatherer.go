package defs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/21S1298001/mahiron/internal/epggather"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/job/run"
)

const (
	EPGGathererKey  = "epg-gatherer"
	EPGGathererName = "EPG Gatherer"

	EPGGathererDefaultSchedule = "20,50 * * * *"
	epgRefreshInterval         = 30 * time.Minute
)

func RegisterEPGGatherer(registry Registry, service EPGGatherer, programs ProgramCleaner, retentionDays int) {
	definition := job.JobDefinition{
		Key:  EPGGathererKey,
		Name: EPGGathererName,
		Handler: epgGathererHandler(registry, service, func(ctx context.Context) error {
			return programs.DeleteExpired(ctx, time.Now(), retentionDays)
		}),
		ExclusiveKeys: []string{"epg-service-topology"},
		IsRerunnable:  true,
		RetryDelays:   []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
	}
	if _, continuous := service.(interface{ Refresh() }); continuous {
		definition.ExclusiveKeys = nil
	}
	registry.Register(definition)
}

// epgGathererHandler dispatches EPG gather groups, then runs
// cleanup when it is set.
func epgGathererHandler(registry Registry, service EPGGatherer, cleanup func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if controller, ok := service.(interface{ Refresh() }); ok {
			controller.Refresh()
			if cleanup != nil {
				return cleanup(ctx)
			}
			return nil
		}
		grouped, err := service.Groups(ctx)
		if err != nil {
			return err
		}
		queued := 0
		fresh := 0
		keys := make([]epggather.GroupKey, 0, len(grouped))
		for key := range grouped {
			keys = append(keys, key)
		}
		slices.SortFunc(keys, func(a, b epggather.GroupKey) int {
			if order := cmp.Compare(grouped[a].OldestSuccessAt, grouped[b].OldestSuccessAt); order != 0 {
				return order
			}
			if order := cmp.Compare(a.NetworkID, b.NetworkID); order != 0 {
				return order
			}
			if a.Stored != b.Stored {
				if a.Stored {
					return -1
				}
				return 1
			}
			if order := cmp.Compare(a.StreamID, b.StreamID); order != 0 {
				return order
			}
			if order := cmp.Compare(a.Channel.Type, b.Channel.Type); order != 0 {
				return order
			}
			return cmp.Compare(a.Channel.Channel, b.Channel.Channel)
		})
		cutoff := time.Now().Add(-epgRefreshInterval).UnixMilli()
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			group := grouped[key]
			if group.OldestSuccessAt > cutoff {
				fresh++
				continue
			}
			enqueued, err := enqueueEPGGatherGroup(registry, service, key, group)
			if err != nil {
				return err
			}
			if enqueued {
				queued++
			}
		}
		slog.Info("EPG gatherer dispatched", "groups", len(grouped), "queued", queued, "fresh", fresh)

		result := run.Result{
			Kind:    "epg_gatherer",
			Summary: fmt.Sprintf("%d/%d groups queued", queued, len(grouped)),
			Counts: map[string]int{
				"groups": len(grouped),
				"queued": queued,
				"fresh":  fresh,
			},
		}
		if cleanup != nil {
			if err := cleanup(ctx); err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("cleanup failed: %v", err))
				slog.Warn("failed to clean up old EPG data", "err", err)
			} else {
				result.Counts["cleanupSucceeded"] = 1
				slog.Debug("EPG cleanup completed")
			}
		}
		run.Set(ctx, result)
		return nil
	}
}

func enqueueEPGGatherGroup(registry Registry, service EPGGatherer, key epggather.GroupKey, group *epggather.Group) (bool, error) {
	if group == nil || len(group.Services) == 0 {
		return false, nil
	}
	definition := epgGatherDefinition(service, key, group)
	if _, err := registry.EnqueueDefinition(definition); err != nil {
		if errors.Is(err, job.ErrJobAlreadyRunning) {
			return false, nil
		}
		return false, err
	}
	slog.Info("EPG gather queued", "networkId", key.NetworkID, "streamId", key.StreamID, "stored", key.Stored, "candidates", len(group.Candidates), "services", len(group.Services))
	return true, nil
}

func epgGatherDefinition(service EPGGatherer, key epggather.GroupKey, group *epggather.Group) job.JobDefinition {
	jobKey := fmt.Sprintf("epg-gather:nid:%d", key.NetworkID)
	name := "EPG Gather"
	if len(group.Candidates) > 0 {
		candidate := group.Candidates[0]
		name += fmt.Sprintf(" %s/%s", candidate.Type, candidate.Channel)
		if len(group.Candidates) > 1 {
			name += fmt.Sprintf(" (+%d)", len(group.Candidates)-1)
		}
	}
	if !key.Stored {
		jobKey += fmt.Sprintf(":stream:%d", key.StreamID)
		if key.Channel != (epggather.Candidate{}) {
			jobKey += fmt.Sprintf(":channel:%s:%s", key.Channel.Type, key.Channel.Channel)
		}
	}
	return job.JobDefinition{
		Key: jobKey, Name: name, IsRerunnable: true,
		SharedKeys:  []string{"epg-service-topology"},
		Priority:    -group.OldestSuccessAt,
		Handler:     func(ctx context.Context) error { return service.GatherGroup(ctx, key) },
		RetryDelays: []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
		RetryIf:     epggather.RetryableError,
	}
}

func compareGroupKeys(a, b epggather.GroupKey) int {
	if order := cmp.Compare(a.NetworkID, b.NetworkID); order != 0 {
		return order
	}
	if a.Stored != b.Stored {
		if a.Stored {
			return -1
		}
		return 1
	}
	if order := cmp.Compare(a.StreamID, b.StreamID); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Channel.Type, b.Channel.Type); order != 0 {
		return order
	}
	return cmp.Compare(a.Channel.Channel, b.Channel.Channel)
}
