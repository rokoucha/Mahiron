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
	registry.Register(job.JobDefinition{
		Key:  EPGGathererKey,
		Name: EPGGathererName,
		Handler: epgGathererHandler(registry, service, func(ctx context.Context) error {
			return programs.DeleteExpired(ctx, time.Now(), retentionDays)
		}),
		ExclusiveKeys: []string{"epg-service-topology"},
		IsRerunnable:  true,
		RetryDelays:   []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
	})
}

// epgGathererHandler dispatches the per-network EPG gather jobs, then runs
// cleanup when it is set.
func epgGathererHandler(registry Registry, service EPGGatherer, cleanup func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		grouped, err := service.Groups(ctx)
		if err != nil {
			return err
		}
		queued := 0
		fresh := 0
		networkIDs := make([]uint16, 0, len(grouped))
		for nid := range grouped {
			networkIDs = append(networkIDs, nid)
		}
		slices.SortFunc(networkIDs, func(a, b uint16) int {
			if order := cmp.Compare(grouped[a].OldestSuccessAt, grouped[b].OldestSuccessAt); order != 0 {
				return order
			}
			return cmp.Compare(a, b)
		})
		cutoff := time.Now().Add(-epgRefreshInterval).UnixMilli()
		for _, nid := range networkIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			group := grouped[nid]
			if group.OldestSuccessAt > cutoff {
				fresh++
				continue
			}
			enqueued, err := enqueueEPGGatherForNetwork(ctx, registry, service, nid, group)
			if err != nil {
				return err
			}
			if enqueued {
				queued++
			}
		}
		slog.Info("EPG gatherer dispatched", "networks", len(grouped), "queued", queued, "fresh", fresh)

		result := run.Result{
			Kind:    "epg_gatherer",
			Summary: fmt.Sprintf("%d/%d networks queued", queued, len(grouped)),
			Counts: map[string]int{
				"networks": len(grouped),
				"queued":   queued,
				"fresh":    fresh,
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

// enqueueEPGGatherForNetwork skips networks without services or with an active
// job. A nil network loads the current inputs before enqueueing.
func enqueueEPGGatherForNetwork(ctx context.Context, registry Registry, service EPGGatherer, networkID uint16, network *epggather.Network) (bool, error) {
	if network == nil {
		var err error
		network = &epggather.Network{}
		network.Candidates, network.Services, err = service.BuildNetworkInputs(ctx, networkID)
		if err != nil {
			return false, err
		}
	}
	if len(network.Services) == 0 {
		slog.Debug("skipping EPG gather enqueue with no services", "networkId", networkID)
		return false, nil
	}
	nid := networkID
	definition := job.JobDefinition{
		Key: fmt.Sprintf("epg-gather:nid:%d", nid), Name: fmt.Sprintf("EPG Gather NID %d", nid), IsRerunnable: true,
		SharedKeys: []string{"epg-service-topology"},
		Priority:   -network.OldestSuccessAt,
		Handler: func(childCtx context.Context) error {
			// A service scan may have changed the topology while this job waited.
			candidates, services, err := service.BuildNetworkInputs(childCtx, nid)
			if err != nil {
				return err
			}
			if len(services) == 0 {
				return nil
			}
			return service.GatherNetwork(childCtx, nid, candidates, services)
		},
		RetryDelays: []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
		RetryIf:     epggather.RetryableError,
	}
	if _, err := registry.EnqueueDefinition(definition); err != nil {
		if errors.Is(err, job.ErrJobAlreadyRunning) {
			slog.Debug("EPG gather already queued or running", "networkId", networkID)
			return false, nil
		}
		return false, err
	}
	slog.Info("EPG gather queued", "networkId", networkID, "candidates", len(network.Candidates), "services", len(network.Services))
	return true, nil
}
