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
	// StoredEPG reports whether every enabled route reads stored programs.
	StoredEPG(channelType, channelID string) bool
	// OpenSchedule acquires the channel. A channel served by a remote
	// Mahiron or Mirakurun returns listStored, whose stored programs are
	// copied; any other channel returns collect.
	OpenSchedule(ctx context.Context, channelType, channelID string) (collect CollectSchedule, listStored ListStoredPrograms, err error)
}

type Gatherer struct {
	channels      config.ChannelsConfig
	events        ScheduleWriter
	programStore  ProgramStore
	retrievalTime time.Duration
	serviceStore  ServiceStore
	streams       StreamManager
}

func NewGatherer(events ScheduleWriter, programStore ProgramStore, serviceStore ServiceStore, streams StreamManager, channels config.ChannelsConfig, retrievalTime time.Duration) *Gatherer {
	return &Gatherer{
		channels:      channels,
		events:        events,
		programStore:  programStore,
		retrievalTime: retrievalTime,
		serviceStore:  serviceStore,
		streams:       streams,
	}
}

func (s *Gatherer) Groups(ctx context.Context) (map[GroupKey]*Group, error) {
	storedServices, err := s.serviceStore.GetServices(ctx)
	if err != nil {
		return nil, fmt.Errorf("get services: %w", err)
	}
	if len(storedServices) == 0 {
		return nil, errors.New("EPG gathering requires scanned services")
	}
	return groupServices(storedServices, s.channels, s.streams.StoredEPG), nil
}

func (s *Gatherer) GatherGroup(ctx context.Context, key GroupKey) error {
	// Scans may have changed the topology while the job waited.
	storedServices, err := s.serviceStore.GetServices(ctx)
	if err != nil {
		return fmt.Errorf("get services: %w", err)
	}
	group := groupServices(storedServices, s.channels, s.streams.StoredEPG)[key]
	if group == nil {
		return nil
	}
	return gatherNetwork(ctx, s.events, s.programStore, s.serviceStore, s.streams, key.NetworkID, group.Candidates, group.Services, s.retrievalTime)
}

func RetryableError(err error) bool {
	return err != nil && !errors.Is(err, ErrIncomplete)
}

// CheckGroup is only a hint; OpenSchedule must still acquire without waiting.
func (s *Gatherer) CheckGroup(ctx context.Context, key GroupKey) error {
	if key.Stored {
		return nil
	}
	checker, ok := s.streams.(interface {
		CheckAvailable(context.Context, string, string) error
	})
	if !ok {
		return nil
	}
	groups, err := s.Groups(ctx)
	if err != nil {
		return err
	}
	group := groups[key]
	if group == nil {
		return nil
	}
	var result error
	for _, candidate := range group.Candidates {
		if s.streams.HasSession(candidate.Type, candidate.Channel) {
			return nil
		}
		if err := checker.CheckAvailable(ctx, candidate.Type, candidate.Channel); err == nil {
			return nil
		} else {
			result = errors.Join(result, err)
		}
	}
	return result
}

var ErrIncomplete = errors.New("EPG reception incomplete")
var ErrDeferred = errors.New("EPG acquisition deferred: no available tuner")
