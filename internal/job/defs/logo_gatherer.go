package defs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/logogather"
)

const (
	LogoGathererKey             = "logo-gatherer"
	LogoGathererName            = "Logo Gatherer"
	LogoGathererDefaultSchedule = "5 3 * * *"
)

func RegisterLogoGatherer(registry Registry, gatherer LogoGatherer) {
	registry.Register(job.JobDefinition{
		Key: LogoGathererKey, Name: LogoGathererName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			targets, err := gatherer.Targets(ctx)
			if err != nil {
				return err
			}
			queued, err := enqueueLogoGatherTargets(ctx, registry, gatherer, targets, true)
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

// enqueueLogoGatherTargets queues one gather job per channel, re-gathering
// common data targets from the channel a probe's announcement named.
func enqueueLogoGatherTargets(ctx context.Context, registry Registry, gatherer LogoGatherer, targets []logogather.Target, allowProbeRefresh bool) (int, error) {
	grouped := make(map[string][]logogather.Target)
	var order []string
	for _, target := range targets {
		key := target.ChannelType + "\x00" + target.ChannelID
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], target)
	}
	queued := 0
	for _, key := range order {
		if err := ctx.Err(); err != nil {
			return queued, err
		}
		channelTargets := grouped[key]
		channelType, channelID := channelTargets[0].ChannelType, channelTargets[0].ChannelID
		hasProbe := false
		for _, target := range channelTargets {
			hasProbe = hasProbe || target.Probe
		}
		definition := job.JobDefinition{
			Key:          fmt.Sprintf("logo-gather:%s:%s", channelType, channelID),
			Name:         fmt.Sprintf("Logo Gather %s/%s", channelType, channelID),
			IsRerunnable: true,
			Handler: func(childCtx context.Context) error {
				if err := gatherer.GatherChannel(childCtx, channelType, channelID, channelTargets); err != nil {
					return err
				}
				if !hasProbe || !allowProbeRefresh {
					return nil
				}
				refreshed, err := gatherer.Targets(childCtx)
				if err != nil {
					return err
				}
				_, err = enqueueLogoGatherTargets(childCtx, registry, gatherer, logogather.ResolvedCommonData(refreshed), false)
				return err
			},
		}
		if _, err := registry.EnqueueDefinition(definition); err != nil {
			if errors.Is(err, job.ErrJobAlreadyRunning) {
				continue
			}
			return queued, err
		}
		queued++
	}
	return queued, nil
}
