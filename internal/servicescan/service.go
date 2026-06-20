package servicescan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	"github.com/google/uuid"
)

type Store interface {
	GetByChannel(ctx context.Context, channelType, channelID string) ([]*service.Service, error)
	ReplaceChannelServices(ctx context.Context, channelType, channelID string, services []*service.Service) error
}

type StreamScanner interface {
	ScanServices(context.Context, string, string, bool, io.Writer) error
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

type scanService struct {
	Nid                uint16 `json:"nid"`
	Tsid               uint16 `json:"tsid"`
	Sid                uint16 `json:"sid"`
	Name               string `json:"name"`
	Type               uint8  `json:"type"`
	LogoId             uint64 `json:"logoId"`
	RemoteControlKeyId uint8  `json:"remoteControlKeyId"`
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
		if isDisabled(channel) {
			continue
		}
		channels = append(channels, Channel{Type: channel.Type, ID: channel.Channel})
	}
	return channels
}

func (s *Service) ScanChannel(ctx context.Context, channelType string, channelID string, wait bool) ([]uint16, error) {
	startedAt := time.Now()
	slog.Info("service scan started", "type", channelType, "channel", channelID, "wait", wait)
	existing, err := s.store.GetByChannel(ctx, channelType, channelID)
	if err != nil {
		return nil, fmt.Errorf("list existing services: %w", err)
	}
	before := make(map[string]struct{}, len(existing))
	for _, svc := range existing {
		before[svc.Id] = struct{}{}
	}

	out := bytes.Buffer{}
	yes := true
	ctx = tuner.WithUser(ctx, tuner.User{
		ID: uuid.NewString(), Priority: -1, Agent: "Mahiron Service Scanner",
		StreamSetting: tuner.StreamSetting{
			Channel:  &config.ChannelConfig{Type: channelType, Channel: channelID},
			ParseNIT: &yes, ParseSDT: &yes,
		},
	})

	if err := s.scanner.ScanServices(ctx, channelType, channelID, wait, &out); err != nil {
		slog.Warn("service scan failed", "type", channelType, "channel", channelID, "duration", time.Since(startedAt), "err", err)
		return nil, err
	}

	var services []*scanService
	if err := json.Unmarshal(out.Bytes(), &services); err != nil {
		slog.Warn("failed to decode service scan result", "type", channelType, "channel", channelID, "bytes", out.Len(), "err", err)
		return nil, err
	}

	scanned := make([]*service.Service, len(services))
	for i, svc := range services {
		scanned[i] = &service.Service{
			Id:                 fmt.Sprintf("%05d%05d", svc.Nid, svc.Sid),
			ServiceId:          svc.Sid,
			NetworkId:          svc.Nid,
			TransportStreamId:  svc.Tsid,
			Name:               svc.Name,
			Type:               svc.Type,
			RemoteControlKeyId: svc.RemoteControlKeyId,
			ChannelType:        channelType,
			ChannelId:          channelID,
		}
	}

	if err := s.store.ReplaceChannelServices(ctx, channelType, channelID, scanned); err != nil {
		return nil, err
	}

	newNIDs := newNetworkIDsFromDiff(before, scanned)
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

func isDisabled(channel config.ChannelConfig) bool {
	return channel.IsDisabled != nil && *channel.IsDisabled
}
