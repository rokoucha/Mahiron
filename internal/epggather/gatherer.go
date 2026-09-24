package epggather

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

type ServiceStore interface {
	GetServices(context.Context) ([]*service.Service, error)
	SetEPGAttempt(context.Context, uint16, uint16, int64, string) error
	SetEPGSuccess(context.Context, uint16, uint16, int64) error
}

// ListStoredPrograms lists a remote server's stored programs of a service.
type ListStoredPrograms = func(ctx context.Context, networkID, serviceID uint16) ([]model.Event, error)

// StreamManager gives EPG gathering access to channel sessions. It uses only
// model and standard types (the aliases above), so that gathering does not
// depend on the stream package; stream.EPGGatherAdapter implements it.
type StreamManager interface {
	HasSession(channelType, channelID string) bool
	// NetworkWideEIT reports whether every stream of the network carries
	// the whole network's EIT schedule, as TS satellite streams do.
	NetworkWideEIT(networkID uint16) bool
	// OpenSchedule acquires the channel. A channel served by a remote
	// Mahiron or Mirakurun returns listStored, whose stored programs are
	// copied; any other channel returns collect.
	OpenSchedule(ctx context.Context, channelType, channelID string) (collect CollectSchedule, listStored ListStoredPrograms, err error)
}

type Gatherer struct {
	channels      config.ChannelsConfig
	events        EventWriter
	programStore  ProgramStore
	retrievalTime time.Duration
	serviceStore  ServiceStore
	streams       StreamManager
}

func NewGatherer(events EventWriter, programStore ProgramStore, serviceStore ServiceStore, streams StreamManager, channels config.ChannelsConfig, retrievalTime time.Duration) *Gatherer {
	return &Gatherer{
		channels:      channels,
		events:        events,
		programStore:  programStore,
		retrievalTime: retrievalTime,
		serviceStore:  serviceStore,
		streams:       streams,
	}
}

func (s *Gatherer) Groups(ctx context.Context) (map[uint16]*Network, error) {
	storedServices, err := s.serviceStore.GetServices(ctx)
	if err != nil {
		return nil, fmt.Errorf("get services: %w", err)
	}
	if len(storedServices) == 0 {
		return nil, errors.New("EPG gathering requires scanned services")
	}
	return groupServicesByNetwork(storedServices, s.channels, s.streams.NetworkWideEIT), nil
}

func (s *Gatherer) BuildNetworkInputs(ctx context.Context, networkID uint16) ([]Candidate, []model.ServiceKey, error) {
	return buildNetworkInputs(ctx, s.serviceStore, s.channels, networkID, s.streams.NetworkWideEIT)
}

func (s *Gatherer) GatherNetwork(ctx context.Context, networkID uint16, candidates []Candidate, serviceKeys []model.ServiceKey) error {
	return gatherNetwork(ctx, s.events, s.programStore, s.serviceStore, s.streams, networkID, candidates, serviceKeys, s.retrievalTime)
}

func RetryableError(err error) bool {
	return err != nil
}
