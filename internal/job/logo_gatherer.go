package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/ts"
)

const (
	LogoGathererKey             = "logo-gatherer"
	LogoGathererName            = "Logo Gatherer"
	LogoGathererDefaultSchedule = "5 3 * * *"
)

var errLogoTargetsComplete = errors.New("logo targets complete")

func RegisterLogoGatherer(registry Registry, collector LogoCollector, store LogoStore, timeout time.Duration) {
	if timeout <= 0 {
		timeout = 20 * time.Minute
	}
	registry.Register(JobDefinition{
		Key: LogoGathererKey, Name: LogoGathererName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			targets, err := logoGatherTargets(ctx, store)
			if err != nil {
				return err
			}
			queued, err := enqueueLogoGatherTargets(ctx, registry, collector, store, timeout, targets, true)
			if err != nil {
				return err
			}
			run.Set(ctx, run.Result{
				Kind:    "logo_gatherer",
				Summary: fmt.Sprintf("%d channels queued for %d targets", queued, len(targets)),
				Counts: map[string]int{
					"targets": len(targets),
					"queued":  queued,
				},
			})
			slog.Info("logo gatherer dispatched", "queued", queued)
			return nil
		},
	})
}

func enqueueLogoGatherTargets(ctx context.Context, registry Registry, collector LogoCollector, store LogoStore, timeout time.Duration, targets []service.LogoTarget, allowProbeRefresh bool) (int, error) {
	grouped := make(map[string][]service.LogoTarget)
	for _, target := range targets {
		key := target.ChannelType + "\x00" + target.ChannelId
		grouped[key] = append(grouped[key], target)
	}
	queued := 0
	for _, channelTargets := range grouped {
		if err := ctx.Err(); err != nil {
			return queued, err
		}
		channelTargets := append([]service.LogoTarget(nil), channelTargets...)
		channelType, channelID := channelTargets[0].ChannelType, channelTargets[0].ChannelId
		hasProbe := false
		for _, target := range channelTargets {
			hasProbe = hasProbe || target.IsSDTTProbe
		}
		definition := JobDefinition{
			Key:          fmt.Sprintf("logo-gather:%s:%s", channelType, channelID),
			Name:         fmt.Sprintf("Logo Gather %s/%s", channelType, channelID),
			IsRerunnable: true,
			Handler: func(childCtx context.Context) error {
				gatherCtx, cancel := context.WithTimeout(childCtx, timeout)
				defer cancel()
				remaining := make(map[logoTargetKey]struct{}, len(channelTargets))
				for _, target := range channelTargets {
					if target.IsCommonData {
						continue
					}
					remaining[newLogoTargetKey(target)] = struct{}{}
				}
				count := 0
				hasRemainingTargets := len(remaining) > 0
				err := collector.ObserveLogos(gatherCtx, channelType, channelID, func(image *ts.LogoImage) error {
					// Local sessions persist CDT logos as they are decoded. Remote
					// sessions obtain the same images through the API, so persist here
					// as well to keep both acquisition paths equivalent.
					if err := store.UpsertLogoImage(gatherCtx, image); err != nil {
						return err
					}
					if image.IsDeleted {
						return nil
					}
					count++
					delete(remaining, logoTargetKey{int64(image.OriginalNetworkID), int64(image.LogoID), int64(image.LogoVersion), int64(image.DownloadDataID)})
					if hasRemainingTargets && len(remaining) == 0 {
						return errLogoTargetsComplete
					}
					return nil
				})
				timedOut := errors.Is(err, context.DeadlineExceeded) || (errors.Is(err, context.Canceled) && childCtx.Err() == nil)
				if errors.Is(err, errLogoTargetsComplete) || timedOut {
					err = nil
				}
				if err != nil {
					return err
				}
				if hasProbe && allowProbeRefresh {
					refreshed, err := logoGatherTargets(childCtx, store)
					if err != nil {
						return err
					}
					if _, err := enqueueLogoGatherTargets(childCtx, registry, collector, store, timeout, resolvedCommonLogoTargets(refreshed), false); err != nil {
						return err
					}
				}
				run.Set(childCtx, logoGatherResult(channelType, channelID, channelTargets, count, len(remaining), timedOut))
				slog.Info("logo gather completed", "channel", fmt.Sprintf("%s/%s", channelType, channelID), "logos", count, "remaining", len(remaining), "timeout", timeout)
				return nil
			},
		}
		if _, err := registry.EnqueueDefinition(definition); err != nil {
			if errors.Is(err, ErrJobAlreadyRunning) {
				continue
			}
			return queued, err
		}
		queued++
	}
	return queued, nil
}

func logoGatherResult(channelType, channelID string, targets []service.LogoTarget, logos, remaining int, timedOut bool) run.Result {
	items := make([]run.Item, 0, len(targets))
	for _, target := range targets {
		items = append(items, run.Item{
			Kind:    "logo_target",
			Summary: fmt.Sprintf("service %d logo %d", target.ServiceId, target.LogoId),
			Data: map[string]any{
				"networkId":      target.NetworkId,
				"serviceId":      target.ServiceId,
				"logoId":         target.LogoId,
				"logoVersion":    target.LogoVersion,
				"downloadDataId": target.LogoDownloadDataId,
				"isCommonData":   target.IsCommonData,
				"isSDTTProbe":    target.IsSDTTProbe,
			},
		})
	}
	warnings := []string(nil)
	if timedOut && remaining > 0 {
		warnings = append(warnings, "logo gathering reached timeout before all targets were observed")
	}
	return run.Result{
		Kind:    "logo_gather",
		Summary: fmt.Sprintf("%s/%s: %d logos observed, %d remaining", channelType, channelID, logos, remaining),
		Counts: map[string]int{
			"targets":   len(targets),
			"logos":     logos,
			"remaining": remaining,
			"timedOut":  boolCount(timedOut),
		},
		Items:    items,
		Warnings: warnings,
	}
}

func boolCount(v bool) int {
	if v {
		return 1
	}
	return 0
}

func resolvedCommonLogoTargets(targets []service.LogoTarget) []service.LogoTarget {
	result := make([]service.LogoTarget, 0, len(targets))
	for _, target := range targets {
		if target.IsCommonData && target.IsSDTTProbe {
			continue
		}
		if target.IsCommonData {
			result = append(result, target)
		}
	}
	return result
}

func logoGatherTargets(ctx context.Context, store LogoStore) ([]service.LogoTarget, error) {
	if gatherStore, ok := store.(LogoGatherTargetStore); ok {
		return gatherStore.LogoGatherTargets(ctx)
	}
	return store.MissingLogoTargets(ctx)
}

type logoTargetKey struct {
	networkID, logoID, logoVersion, downloadDataID int64
}

func newLogoTargetKey(target service.LogoTarget) logoTargetKey {
	return logoTargetKey{int64(target.NetworkId), target.LogoId, target.LogoVersion, target.LogoDownloadDataId}
}
