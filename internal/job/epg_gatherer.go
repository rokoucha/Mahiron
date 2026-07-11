package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/job/run"
)

const (
	EPGGathererKey  = "epg-gatherer"
	EPGGathererName = "EPG Gatherer"

	EPGGathererDefaultSchedule = "20,50 * * * *"
)

func RegisterEPGGatherer(registry Registry, programStore EPGProgramStore, serviceStore EPGServiceStore, epgStreams EPGStreamManager, channels config.ChannelsConfig, epgRetentionDays int, retrievalTime time.Duration) {
	service := epg.NewService(programStore, serviceStore, epgStreams, channels, epgRetentionDays, retrievalTime)
	RegisterEPGGathererService(registry, service)
}

func RegisterEPGGathererService(registry Registry, service EPGGatherer) {
	registry.Register(JobDefinition{
		Key:           EPGGathererKey,
		Name:          EPGGathererName,
		Handler:       epgGathererHandler(registry, service),
		ExclusiveKeys: []string{"epg-service-topology"},
		IsRerunnable:  true,
		RetryDelays:   []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
	})
}

func epgGathererHandler(registry Registry, service EPGGatherer) func(context.Context) error {
	return func(ctx context.Context) error {
		grouped, err := service.Groups(ctx)
		if err != nil {
			return err
		}
		queued := 0
		for nid, group := range grouped {
			if err := ctx.Err(); err != nil {
				return err
			}
			enqueued, err := enqueueEPGGatherForNetwork(ctx, registry, service, nid, group.Candidates, group.Services)
			if err != nil {
				return err
			}
			if enqueued {
				queued++
			}
		}
		slog.Info("EPG gatherer dispatched", "networks", len(grouped), "queued", queued)

		result := run.Result{
			Kind:    "epg_gatherer",
			Summary: fmt.Sprintf("%d/%d networks queued", queued, len(grouped)),
			Counts: map[string]int{
				"networks": len(grouped),
				"queued":   queued,
			},
		}
		if err := service.Cleanup(ctx, time.Now()); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("cleanup failed: %v", err))
			slog.Warn("failed to clean up old EPG data", "err", err)
		} else {
			result.Counts["cleanupSucceeded"] = 1
			slog.Debug("EPG cleanup completed")
		}
		run.Set(ctx, result)
		return nil
	}
}

// enqueueEPGGatherForNetwork enqueues the per-network EPG gather job for the
// given network ID, ignoring ErrJobAlreadyRunning. It is used by both the
// EPGGatherer cron handler and by callers (e.g. the service updater) that
// want to trigger gathering for a freshly discovered network without waiting
// for the next cron tick. Returns true when a job was actually enqueued (not
// already running and not skipped for having no services).
func enqueueEPGGatherForNetwork(ctx context.Context, registry Registry, service EPGGatherer, networkID uint16, presetCandidates []epg.Candidate, presetServices []epg.ServiceKey) (bool, error) {
	candidates := presetCandidates
	serviceKeys := presetServices
	if len(candidates) == 0 && len(serviceKeys) == 0 {
		var err error
		candidates, serviceKeys, err = service.BuildNetworkInputs(ctx, networkID)
		if err != nil {
			return false, err
		}
	}
	if len(serviceKeys) == 0 {
		slog.Debug("skipping EPG gather enqueue with no services", "networkId", networkID)
		return false, nil
	}
	nid := networkID
	networkCandidates := append([]epg.Candidate(nil), candidates...)
	networkServices := append([]epg.ServiceKey(nil), serviceKeys...)
	definition := JobDefinition{
		Key: fmt.Sprintf("epg-gather:nid:%d", nid), Name: fmt.Sprintf("EPG Gather NID %d", nid), IsRerunnable: true,
		ExclusiveKeys: []string{"epg-service-topology"},
		Handler: func(childCtx context.Context) error {
			return service.GatherNetwork(childCtx, nid, networkCandidates, networkServices)
		},
		RetryDelays: []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
		RetryIf:     epg.RetryableError,
	}
	if _, err := registry.EnqueueDefinition(definition); err != nil {
		if errors.Is(err, ErrJobAlreadyRunning) {
			slog.Debug("EPG gather already queued or running", "networkId", networkID)
			return false, nil
		}
		return false, err
	}
	slog.Info("EPG gather queued", "networkId", networkID, "candidates", len(networkCandidates), "services", len(networkServices))
	return true, nil
}
