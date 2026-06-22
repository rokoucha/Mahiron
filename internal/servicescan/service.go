package servicescan

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/observability"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	"github.com/21S1298001/Mahiron5/ts"
	"github.com/google/uuid"
)

type Store interface {
	GetServicesByChannel(ctx context.Context, channelType, channelID string) ([]*service.Service, error)
	ReplaceChannelServices(ctx context.Context, channelType, channelID string, services []*service.Service) error
}

type StreamScanner interface {
	ScanServices(context.Context, string, string, bool) ([]ts.ServiceInfo, error)
}

type Service struct {
	channels config.ChannelsConfig
	scanner  StreamScanner
	store    Store
}

type Channel struct {
	Type string
	ID   string
}

func NewService(store Store, scanner StreamScanner, channels config.ChannelsConfig) *Service {
	return &Service{
		channels: channels,
		scanner:  scanner,
		store:    store,
	}
}

func (s *Service) Channels() []Channel {
	channels := make([]Channel, 0, len(s.channels))
	for _, channel := range s.channels {
		if config.IsChannelDisabled(channel) {
			continue
		}
		channels = append(channels, Channel{Type: channel.Type, ID: channel.Channel})
	}
	return channels
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
	for _, svc := range existing {
		before[svc.Id] = struct{}{}
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
	services, err := s.scanner.ScanServices(scanCtx, channelType, channelID, wait)
	observability.EndSpan(scanSpan, err)
	if err != nil {
		slog.Warn("service scan failed", "type", channelType, "channel", channelID, "duration", time.Since(startedAt), "err", err)
		return nil, err
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
			Id:                 fmt.Sprintf("%05d%05d", svc.Nid, svc.Sid),
			ServiceId:          svc.Sid,
			NetworkId:          svc.Nid,
			TransportStreamId:  svc.Tsid,
			Name:               svc.Name,
			Type:               svc.Type,
			LogoId:             logoID,
			LogoVersion:        logoVersion,
			LogoDownloadDataId: logoDownloadDataID,
			RemoteControlKeyId: remoteControlKeyID,
			ChannelType:        channelType,
			ChannelId:          channelID,
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
	slog.Info("service scan completed", "type", channelType, "channel", channelID, "services", len(scanned), "newNetworks", len(newNIDs), "duration", time.Since(startedAt))
	return newNIDs, nil
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
