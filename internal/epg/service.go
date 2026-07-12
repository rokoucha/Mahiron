package epg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
)

type ServiceStore interface {
	GetServices(context.Context) ([]*service.Service, error)
	SetEPGAttempt(context.Context, uint16, uint16, int64, string) error
	SetEPGSuccess(context.Context, uint16, uint16, int64) error
}

type StreamManager interface {
	HasSession(string, string) bool
	GetOrCreateWait(context.Context, string, string) (stream.Session, error)
}

type StoredProgramLister interface {
	ListServicePrograms(context.Context, uint16, uint16) ([]*program.Program, error)
}

type Service struct {
	channels      config.ChannelsConfig
	programStore  ProgramStore
	retentionDays int
	retrievalTime time.Duration
	serviceStore  ServiceStore
	streams       StreamManager
}

func NewService(programStore ProgramStore, serviceStore ServiceStore, streams StreamManager, channels config.ChannelsConfig, retentionDays int, retrievalTime time.Duration) *Service {
	return &Service{
		channels:      channels,
		programStore:  programStore,
		retentionDays: retentionDays,
		retrievalTime: retrievalTime,
		serviceStore:  serviceStore,
		streams:       streams,
	}
}

func (s *Service) Groups(ctx context.Context) (map[uint16]*Network, error) {
	storedServices, err := s.serviceStore.GetServices(ctx)
	if err != nil {
		return nil, fmt.Errorf("get services: %w", err)
	}
	if len(storedServices) == 0 {
		return nil, errors.New("EPG gathering requires scanned services")
	}
	return groupServicesByNetwork(storedServices, s.channels), nil
}

func (s *Service) BuildNetworkInputs(ctx context.Context, networkID uint16) ([]Candidate, []ServiceKey, error) {
	return buildNetworkInputs(ctx, s.serviceStore, s.channels, networkID)
}

func (s *Service) GatherNetwork(ctx context.Context, networkID uint16, candidates []Candidate, serviceKeys []ServiceKey) error {
	return gatherNetwork(ctx, s.programStore, s.serviceStore, s.streams, networkID, candidates, serviceKeys, s.retrievalTime)
}

func (s *Service) Cleanup(ctx context.Context, now time.Time) error {
	if s.retentionDays <= 0 {
		slog.Debug("skipping EPG cleanup", "retentionDays", s.retentionDays)
		return nil
	}
	cutoff := now.Add(-time.Duration(s.retentionDays) * 24 * time.Hour).UnixMilli()
	slog.Debug("cleaning up old EPG data", "retentionDays", s.retentionDays, "cutoff", cutoff)
	return s.programStore.DeleteEndedBefore(observability.ContextWithEPGMetricSource(ctx, "cleanup"), cutoff)
}

func RetryableError(err error) bool {
	return err != nil
}
