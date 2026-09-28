// Package logogather picks the channels to tune for logos services still
// lack or may have updated, waits for them through stream.LogoGatherAdapter,
// and stores what arrives.
package logogather

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

// DefaultTimeout bounds gathering on one channel. Logos are sent rarely, and
// giving up only costs waiting for the next run.
const DefaultTimeout = 20 * time.Minute

// ServiceStore is where services and their logos live.
type ServiceStore interface {
	GetServices(context.Context) ([]*service.Service, error)
	CommonDataAnnouncements(context.Context) ([]model.CommonDataAnnouncement, error)
	UpsertLogoImage(context.Context, model.Logo) error
}

// Sources reaches the channel sessions and answers the transport questions
// of the ISDB-S all-receivers common data, which carries satellite logos.
type Sources interface {
	ObserveLogos(ctx context.Context, channelType, channelID string, observe func(model.Logo) error) error
	// CommonDataNetwork reports whether the network's services take their
	// logos from the all-receivers common data.
	CommonDataNetwork(networkID uint16) bool
	// DefaultCommonDataService is the service that carries the common data
	// until an announcement names another.
	DefaultCommonDataService() model.ServiceKey
}

// Target is a logo to wait for on a channel.
type Target struct {
	Service     model.ServiceKey
	ChannelType string
	ChannelID   string
	// LogoID, Version and DownloadDataID are the service's logo reference.
	// Common data targets have none: the common data names the services.
	LogoID         uint16
	Version        uint16
	DownloadDataID uint16
	CommonData     bool
	// Probe marks a common data target tuned to its own service's channel
	// while no channel is known to carry the common data yet.
	Probe bool
}

type Gatherer struct {
	store   ServiceStore
	sources Sources
	timeout time.Duration
}

func NewGatherer(store ServiceStore, sources Sources, timeout time.Duration) *Gatherer {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Gatherer{store: store, sources: sources, timeout: timeout}
}

// Targets lists every logo to gather: each service's referenced logo (to
// fetch or notice updates) and the common data of satellite services.
func (g *Gatherer) Targets(ctx context.Context) ([]Target, error) {
	services, err := g.store.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	var targets []Target
	missing := make(map[model.ServiceKey]bool, len(services))
	for _, svc := range services {
		missing[svc.Key] = !svc.HasLogoData
		logo := svc.Logo
		if logo == nil || logo.Version == nil || logo.DownloadDataID == nil {
			continue
		}
		targets = append(targets, Target{
			Service:        svc.Key,
			ChannelType:    svc.ChannelType,
			ChannelID:      svc.ChannelId,
			LogoID:         logo.LogoID,
			Version:        *logo.Version,
			DownloadDataID: *logo.DownloadDataID,
		})
	}
	// Missing logos first, then by channel, as they are dispatched.
	slices.SortStableFunc(targets, func(a, b Target) int {
		if ma, mb := missing[a.Service], missing[b.Service]; ma != mb {
			if ma {
				return -1
			}
			return 1
		}
		return cmp.Or(cmp.Compare(a.ChannelType, b.ChannelType), cmp.Compare(a.ChannelID, b.ChannelID),
			cmp.Compare(a.Service.NetworkID, b.Service.NetworkID), cmp.Compare(a.Service.ServiceID, b.Service.ServiceID))
	})
	common, err := g.commonDataTargets(ctx, services)
	if err != nil {
		return nil, err
	}
	return append(targets, common...), nil
}

// commonDataTargets lists the satellite services still lacking a logo, on
// the channel carrying the common data, or probing their own when none is
// known yet; one target stays even once complete, so updates are noticed.
func (g *Gatherer) commonDataTargets(ctx context.Context, services []*service.Service) ([]Target, error) {
	announcements, err := g.store.CommonDataAnnouncements(ctx)
	if err != nil {
		return nil, err
	}
	byKey := make(map[model.ServiceKey]*service.Service, len(services))
	for _, svc := range services {
		byKey[svc.Key] = svc
	}
	carriers := map[model.ServiceKey]bool{g.sources.DefaultCommonDataService(): true}
	for _, announcement := range announcements {
		carriers[announcement.Service] = true
	}
	channel := g.commonDataChannel(announcements, byKey)

	var targets []Target
	var refresh *Target
	seen := map[Target]bool{}
	for _, svc := range services {
		if !g.sources.CommonDataNetwork(svc.Key.NetworkID) || carriers[svc.Key] {
			continue
		}
		target := Target{Service: svc.Key, ChannelType: svc.ChannelType, ChannelID: svc.ChannelId, CommonData: true, Probe: true}
		if channel != nil {
			target.ChannelType, target.ChannelID, target.Probe = channel.ChannelType, channel.ChannelId, false
			if refresh == nil {
				refresh = &target
			}
		}
		if svc.HasLogoData || seen[target] {
			continue
		}
		targets = append(targets, target)
		seen[target] = true
	}
	if refresh != nil && !seen[*refresh] {
		targets = append(targets, *refresh)
	}
	return targets, nil
}

// commonDataChannel returns the service carrying the common data: the most
// recently announced one that is on a scanned channel, or the default one.
func (g *Gatherer) commonDataChannel(announcements []model.CommonDataAnnouncement, services map[model.ServiceKey]*service.Service) *service.Service {
	for _, announcement := range announcements {
		if !g.sources.CommonDataNetwork(announcement.Service.NetworkID) {
			continue
		}
		if svc := services[announcement.Service]; svc != nil {
			return svc
		}
	}
	return services[g.sources.DefaultCommonDataService()]
}

// ResolvedCommonData keeps the common data targets whose channel is known,
// for gathering again after a probe observed an announcement.
func ResolvedCommonData(targets []Target) []Target {
	var resolved []Target
	for _, target := range targets {
		if target.CommonData && !target.Probe {
			resolved = append(resolved, target)
		}
	}
	return resolved
}

var errTargetsComplete = errors.New("logo targets complete")

type logoKey struct {
	networkID, logoID, version, downloadDataID uint16
}

// GatherChannel stores the logos observed on a channel until every target
// arrived or the timeout passed (not an error); common data targets never
// complete early, since which services they cover is only known as they arrive.
func (g *Gatherer) GatherChannel(ctx context.Context, channelType, channelID string, targets []Target) error {
	gatherCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	remaining := make(map[logoKey]bool, len(targets))
	for _, target := range targets {
		if !target.CommonData {
			remaining[logoKey{target.Service.NetworkID, target.LogoID, target.Version, target.DownloadDataID}] = true
		}
	}
	waitsForTargets := len(remaining) > 0
	count := 0
	err := g.sources.ObserveLogos(gatherCtx, channelType, channelID, func(logo model.Logo) error {
		// Also store here for remote sessions, which unlike local ones don't
		// store logos as they decode them.
		if err := g.store.UpsertLogoImage(gatherCtx, logo); err != nil {
			return err
		}
		if logo.Deleted {
			return nil
		}
		count++
		// A logo that arrived in any of its types completes the target.
		delete(remaining, logoKey{logo.NetworkID, logo.LogoID, logo.Version, logo.DownloadDataID})
		if waitsForTargets && len(remaining) == 0 {
			return errTargetsComplete
		}
		return nil
	})
	timedOut := errors.Is(err, context.DeadlineExceeded) || (errors.Is(err, context.Canceled) && ctx.Err() == nil)
	if errors.Is(err, errTargetsComplete) || timedOut {
		err = nil
	}
	if err != nil {
		return err
	}
	run.Set(ctx, gatherResult(channelType, channelID, targets, count, len(remaining), timedOut))
	slog.Info("logo gather completed", "channel", fmt.Sprintf("%s/%s", channelType, channelID), "logos", count, "remaining", len(remaining), "timeout", g.timeout)
	return nil
}

func gatherResult(channelType, channelID string, targets []Target, logos, remaining int, timedOut bool) run.Result {
	items := make([]run.Item, 0, len(targets))
	for _, target := range targets {
		items = append(items, run.Item{
			Kind:    "logo_target",
			Summary: fmt.Sprintf("service %d logo %d", target.Service.ServiceID, target.LogoID),
			Data: map[string]any{
				"networkId":      target.Service.NetworkID,
				"serviceId":      target.Service.ServiceID,
				"logoId":         target.LogoID,
				"logoVersion":    target.Version,
				"downloadDataId": target.DownloadDataID,
				"isCommonData":   target.CommonData,
				"isSDTTProbe":    target.Probe,
			},
		})
	}
	var warnings []string
	if timedOut && remaining > 0 {
		warnings = append(warnings, "logo gathering reached timeout before all targets were observed")
	}
	timedOutCount := 0
	if timedOut {
		timedOutCount = 1
	}
	return run.Result{
		Kind:    "logo_gather",
		Summary: fmt.Sprintf("%s/%s: %d logos observed, %d remaining", channelType, channelID, logos, remaining),
		Counts: map[string]int{
			"targets":   len(targets),
			"logos":     logos,
			"remaining": remaining,
			"timedOut":  timedOutCount,
		},
		Items:    items,
		Warnings: warnings,
	}
}
