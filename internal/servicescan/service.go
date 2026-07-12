package servicescan

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
	"github.com/google/uuid"
)

type Store interface {
	GetServicesByChannel(ctx context.Context, channelType, channelID string) ([]*service.Service, error)
	ReplaceChannelServices(ctx context.Context, channelType, channelID string, services []*service.Service) error
}

type StreamScanner interface {
	ScanServices(scanCtx, acquireCtx context.Context, channelType, channelID string, wait bool) ([]ts.ServiceInfo, error)
}

type Service struct {
	channels    config.ChannelsConfig
	scanTimeout time.Duration
	scanner     StreamScanner
	store       Store
}

type Channel struct {
	Type string
	ID   string
}

func NewService(store Store, scanner StreamScanner, channels config.ChannelsConfig, scanTimeout time.Duration) *Service {
	return &Service{
		channels:    channels,
		scanTimeout: scanTimeout,
		scanner:     scanner,
		store:       store,
	}
}

func (s *Service) Channels() []Channel {
	seen := make(map[Channel]struct{}, len(s.channels))
	channels := make([]Channel, 0, len(s.channels))
	for _, channel := range s.channels {
		if config.IsChannelDisabled(channel) {
			continue
		}
		key := Channel{Type: channel.Type, ID: channel.Channel}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		channels = append(channels, key)
	}
	return channels
}

// allowedServiceIDs returns the serviceIds configured across all enabled
// (channelType, channelID) entries, and whether scan results should be
// filtered to them. Filtering only applies when every enabled entry for the
// pair specifies a serviceId; a single entry without one means the whole mux
// should be registered, matching prior behavior.
func (s *Service) allowedServiceIDs(channelType, channelID string) (map[uint32]struct{}, bool) {
	allowed := make(map[uint32]struct{})
	matched := false
	for _, channel := range s.channels {
		if channel.Type != channelType || channel.Channel != channelID {
			continue
		}
		if config.IsChannelDisabled(channel) {
			continue
		}
		matched = true
		if channel.ServiceId == nil {
			return nil, false
		}
		allowed[*channel.ServiceId] = struct{}{}
	}
	if !matched {
		return nil, false
	}
	return allowed, true
}

func (s *Service) ScanChannel(ctx context.Context, channelType string, channelID string, wait bool) (newNIDs []uint16, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanServiceScanScanChannel,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channelID),
		observability.AttrWait.Bool(wait),
	)
	defer func() { observability.EndSpan(span, err) }()

	startedAt := time.Now()
	slog.Info("service scan started", "type", channelType, "channel", channelID, "wait", wait)
	existing, err := s.store.GetServicesByChannel(ctx, channelType, channelID)
	if err != nil {
		return nil, fmt.Errorf("list existing services: %w", err)
	}
	before := make(map[string]struct{}, len(existing))
	beforeServices := make(map[string]*service.Service, len(existing))
	for _, svc := range existing {
		before[svc.Id] = struct{}{}
		beforeServices[svc.Id] = svc
	}

	yes := true
	ctx = tuner.WithUser(ctx, tuner.User{
		ID: uuid.NewString(), Priority: -1, Agent: "Mahiron Service Scanner",
		StreamSetting: tuner.StreamSetting{
			Channel:  &config.ChannelConfig{Type: channelType, Channel: channelID},
			ParseNIT: &yes, ParseSDT: &yes,
		},
	})

	scanCtx, scanSpan := observability.StartSpan(ctx, observability.SpanServiceScanRunScanner,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channelID),
	)
	if s.scanTimeout > 0 {
		var cancel context.CancelFunc
		scanCtx, cancel = context.WithTimeout(scanCtx, s.scanTimeout)
		defer cancel()
	}
	services, err := s.scanner.ScanServices(scanCtx, ctx, channelType, channelID, wait)
	observability.EndSpan(scanSpan, err)
	if err != nil {
		slog.Warn("service scan failed", "type", channelType, "channel", channelID, "duration", time.Since(startedAt), "err", err)
		return nil, err
	}

	if allowed, filter := s.allowedServiceIDs(channelType, channelID); filter {
		filtered := make([]ts.ServiceInfo, 0, len(services))
		for _, svc := range services {
			if _, ok := allowed[uint32(svc.Sid)]; ok {
				filtered = append(filtered, svc)
			}
		}
		services = filtered
	}

	scanned := make([]*service.Service, len(services))
	for i, svc := range services {
		var remoteControlKeyID uint8
		if svc.RemoteControlKeyId != nil {
			remoteControlKeyID = *svc.RemoteControlKeyId
		}
		var logoID *int64
		var logoVersion *int64
		var logoDownloadDataID *int64
		if svc.LogoId >= 0 {
			v := svc.LogoId
			logoID = &v
		}
		if svc.LogoVersion != nil {
			v := int64(*svc.LogoVersion)
			logoVersion = &v
		}
		if svc.LogoDownloadDataId != nil {
			v := int64(*svc.LogoDownloadDataId)
			logoDownloadDataID = &v
		}
		scanned[i] = &service.Service{
			Id:                  fmt.Sprintf("%05d%05d", svc.Nid, svc.Sid),
			ServiceId:           svc.Sid,
			NetworkId:           svc.Nid,
			TransportStreamId:   svc.Tsid,
			Name:                svc.Name,
			Type:                svc.Type,
			EITScheduleFlag:     svc.EITScheduleFlag,
			EITPresentFollowing: svc.EITPresentFollowing,
			LogoId:              logoID,
			LogoVersion:         logoVersion,
			LogoDownloadDataId:  logoDownloadDataID,
			RemoteControlKeyId:  remoteControlKeyID,
			ChannelType:         channelType,
			ChannelId:           channelID,
		}
	}

	replaceCtx, replaceSpan := observability.StartSpan(ctx, observability.SpanServiceScanReplaceChannelServices,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channelID),
		observability.AttrServiceCount.Int(len(scanned)),
	)
	err = s.store.ReplaceChannelServices(replaceCtx, channelType, channelID, scanned)
	observability.EndSpan(replaceSpan, err)
	if err != nil {
		return nil, err
	}

	newNIDs = newNetworkIDsFromDiff(before, scanned)
	result := serviceScanResult(channelType, channelID, scanned, beforeServices, newNIDs)
	run.Set(ctx, result)
	span.SetAttributes(
		observability.AttrServiceCount.Int(len(scanned)),
		observability.AttrServiceAdded.Int(result.Counts["addedServices"]),
		observability.AttrServiceRemoved.Int(result.Counts["removedServices"]),
		observability.AttrServiceNewNetworks.Int(len(newNIDs)),
	)
	slog.Info("service scan completed",
		"type", channelType,
		"channel", channelID,
		"services", len(scanned),
		"serviceNames", serviceNames(scanned),
		"newNetworks", len(newNIDs),
		"addedServices", result.Counts["addedServices"],
		"removedServices", result.Counts["removedServices"],
		"duration", time.Since(startedAt))
	return newNIDs, nil
}

func serviceScanResult(channelType, channelID string, scanned []*service.Service, before map[string]*service.Service, newNIDs []uint16) run.Result {
	after := make(map[string]*service.Service, len(scanned))
	items := make([]run.Item, 0, len(scanned)+len(before))
	added := 0
	unchanged := 0
	for _, svc := range scanned {
		after[svc.Id] = svc
		change := "unchanged"
		if _, ok := before[svc.Id]; !ok {
			change = "added"
			added++
		} else {
			unchanged++
		}
		items = append(items, run.Item{
			Kind:    "service",
			Summary: svc.Name,
			Data: map[string]any{
				"networkId":          svc.NetworkId,
				"serviceId":          svc.ServiceId,
				"transportStreamId":  svc.TransportStreamId,
				"name":               svc.Name,
				"type":               svc.Type,
				"remoteControlKeyId": svc.RemoteControlKeyId,
				"hasLogoInfo":        svc.LogoId != nil || svc.LogoVersion != nil || svc.LogoDownloadDataId != nil,
				"change":             change,
			},
		})
	}
	removed := 0
	for id, svc := range before {
		if _, ok := after[id]; ok {
			continue
		}
		removed++
		items = append(items, run.Item{
			Kind:    "service",
			Summary: svc.Name,
			Data: map[string]any{
				"networkId":          svc.NetworkId,
				"serviceId":          svc.ServiceId,
				"transportStreamId":  svc.TransportStreamId,
				"name":               svc.Name,
				"type":               svc.Type,
				"remoteControlKeyId": svc.RemoteControlKeyId,
				"hasLogoInfo":        svc.LogoId != nil || svc.LogoVersion != nil || svc.LogoDownloadDataId != nil,
				"change":             "removed",
			},
		})
	}
	return run.Result{
		Kind:    "service_scan",
		Summary: fmt.Sprintf("%s/%s: %d services (%d added, %d removed)", channelType, channelID, len(scanned), added, removed),
		Counts: map[string]int{
			"services":         len(scanned),
			"addedServices":    added,
			"existingServices": unchanged,
			"removedServices":  removed,
			"newNetworks":      len(newNIDs),
		},
		Items: items,
	}
}

func serviceNames(services []*service.Service) string {
	names := make([]string, 0, len(services))
	for _, svc := range services {
		if svc.Name == "" {
			continue
		}
		names = append(names, svc.Name)
	}
	return strings.Join(names, ", ")
}

// newNetworkIDsFromDiff returns the deduplicated network IDs of services in
// scanned whose service ID was not present in before. Used to detect networks
// that newly appear on a channel after a service scan, so the EPG gatherer can
// be triggered without waiting for its cron schedule.
func newNetworkIDsFromDiff(before map[string]struct{}, scanned []*service.Service) []uint16 {
	if len(scanned) == 0 {
		return nil
	}
	seen := make(map[uint16]struct{})
	var nids []uint16
	for _, svc := range scanned {
		if _, ok := before[svc.Id]; ok {
			continue
		}
		if _, ok := seen[svc.NetworkId]; ok {
			continue
		}
		seen[svc.NetworkId] = struct{}{}
		nids = append(nids, svc.NetworkId)
	}
	return nids
}
