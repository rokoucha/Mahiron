package epg

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	"github.com/google/uuid"
)

type ServiceStore interface {
	GetServices(context.Context) ([]*service.Service, error)
	SetEPGAttempt(context.Context, uint16, uint16, int64, string) error
	SetEPGSuccess(context.Context, uint16, uint16, int64) error
}

type StreamManager interface {
	HasSession(string, string) bool
	GetOrCreate(context.Context, string, string) (interface {
		CollectEITS(context.Context, io.Writer) error
		CollectEITPF(context.Context, io.Writer) error
	}, error)
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

type Candidate struct {
	Type    string
	Channel string
}

type Network struct {
	Candidates []Candidate
	Services   []ServiceKey
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
	return s.programStore.DeleteEndedBefore(ctx, cutoff)
}

func RetryableError(err error) bool {
	return err != nil && !errors.Is(err, ErrMirakcAribRequired)
}

func groupServicesByNetwork(services []*service.Service, channels config.ChannelsConfig) map[uint16]*Network {
	byChannel := make(map[string][]uint16)
	for _, item := range services {
		key := item.ChannelType + "\x00" + item.ChannelId
		byChannel[key] = append(byChannel[key], item.NetworkId)
	}
	groups := make(map[uint16]*Network)
	seen := make(map[uint16]map[string]bool)
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := configured.Type + "\x00" + configured.Channel
		for _, nid := range byChannel[key] {
			if groups[nid] == nil {
				groups[nid] = &Network{}
			}
			if seen[nid] == nil {
				seen[nid] = make(map[string]bool)
			}
			if seen[nid][key] {
				continue
			}
			seen[nid][key] = true
			groups[nid].Candidates = append(groups[nid].Candidates, Candidate{Type: configured.Type, Channel: configured.Channel})
		}
	}
	serviceSeen := make(map[ServiceKey]bool)
	for _, svc := range services {
		key := ServiceKey{NetworkID: svc.NetworkId, ServiceID: svc.ServiceId}
		if groups[svc.NetworkId] != nil && !serviceSeen[key] {
			groups[svc.NetworkId].Services = append(groups[svc.NetworkId].Services, key)
			serviceSeen[key] = true
		}
	}
	return groups
}

func buildNetworkInputs(ctx context.Context, serviceStore ServiceStore, channels config.ChannelsConfig, networkID uint16) ([]Candidate, []ServiceKey, error) {
	storedServices, err := serviceStore.GetServices(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get services: %w", err)
	}
	byChannel := make(map[string]bool)
	for _, item := range storedServices {
		if item.NetworkId != networkID {
			continue
		}
		key := item.ChannelType + "\x00" + item.ChannelId
		byChannel[key] = true
	}
	var candidates []Candidate
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := configured.Type + "\x00" + configured.Channel
		if byChannel[key] {
			candidates = append(candidates, Candidate{Type: configured.Type, Channel: configured.Channel})
		}
	}
	serviceSeen := make(map[ServiceKey]bool)
	var networkServices []ServiceKey
	for _, svc := range storedServices {
		if svc.NetworkId != networkID {
			continue
		}
		key := ServiceKey{NetworkID: svc.NetworkId, ServiceID: svc.ServiceId}
		if !serviceSeen[key] {
			serviceSeen[key] = true
			networkServices = append(networkServices, key)
		}
	}
	return candidates, networkServices, nil
}

func gatherNetwork(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, streams StreamManager, networkID uint16, candidates []Candidate, serviceKeys []ServiceKey, retrievalTime time.Duration) error {
	if len(serviceKeys) == 0 {
		return fmt.Errorf("network %d has no known services", networkID)
	}
	ordered := make([]Candidate, 0, len(candidates))
	active := make(map[Candidate]bool, len(candidates))
	for _, candidate := range candidates {
		if streams.HasSession(candidate.Type, candidate.Channel) {
			active[candidate] = true
			ordered = append(ordered, candidate)
		}
	}
	for _, candidate := range candidates {
		if !active[candidate] {
			ordered = append(ordered, candidate)
		}
	}
	var result error
	for _, candidate := range ordered {
		slog.Info("starting network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "services", len(serviceKeys), "activeSession", active[candidate])
		yes := true
		userCtx := tuner.WithUser(ctx, tuner.User{
			ID: uuid.NewString(), Priority: -1, Agent: "Mahiron EPG Gatherer",
			StreamSetting: tuner.StreamSetting{
				Channel:  &config.ChannelConfig{Type: candidate.Type, Channel: candidate.Channel},
				ParseEIT: &yes,
			},
		})
		session, err := streams.GetOrCreate(userCtx, candidate.Type, candidate.Channel)
		if err == nil {
			err = CollectServiceSnapshots(userCtx, programStore, serviceStore, session, serviceKeys, retrievalTime)
		}
		if err == nil {
			slog.Debug("finished network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("network EPG collection candidate failed", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "err", err)
		result = errors.Join(result, fmt.Errorf("%s/%s: %w", candidate.Type, candidate.Channel, err))
	}
	if result == nil {
		return fmt.Errorf("network %d has no channel candidates", networkID)
	}
	slog.Warn("network EPG collection failed", "networkId", networkID, "candidates", len(ordered), "err", result)
	return result
}

func CollectServiceSnapshots(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, session interface {
	CollectEITS(context.Context, io.Writer) error
	CollectEITPF(context.Context, io.Writer) error
}, expected []ServiceKey, retrievalTime time.Duration) error {
	if len(expected) == 0 {
		return errors.New("collectServiceSnapshots: expected is empty")
	}
	expectedByNID := make(map[uint16]map[uint16]struct{}, len(expected))
	for _, key := range expected {
		if expectedByNID[key.NetworkID] == nil {
			expectedByNID[key.NetworkID] = make(map[uint16]struct{})
		}
		expectedByNID[key.NetworkID][key.ServiceID] = struct{}{}
	}
	matchesExpected := func(section *EITSection) bool {
		ids, ok := expectedByNID[section.OriginalNetworkID]
		if !ok {
			return false
		}
		_, ok = ids[section.ServiceID]
		return ok
	}

	startedAt := time.Now().UnixMilli()
	for _, key := range expected {
		_ = serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, startedAt, "")
	}
	if lister, ok := session.(StoredProgramLister); ok {
		return syncStoredServicePrograms(ctx, programStore, serviceStore, lister, expected, retrievalTime)
	}
	collectCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	eitsR, eitsW := io.Pipe()
	pfR, pfW := io.Pipe()

	collectErrCh := make(chan error, 2)
	go func() {
		collectErrCh <- session.CollectEITS(collectCtx, eitsW)
		_ = eitsW.Close()
	}()
	go func() {
		collectErrCh <- session.CollectEITPF(collectCtx, pfW)
		_ = pfW.Close()
	}()

	type sectionResult struct {
		section *EITSection
		err     error
	}
	sectionCh := make(chan sectionResult, 1)
	go func() {
		defer close(sectionCh)
		scanner := bufio.NewScanner(eitsR)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var section EITSection
			if err := json.Unmarshal(line, &section); err != nil {
				sectionCh <- sectionResult{err: err}
				return
			}
			select {
			case sectionCh <- sectionResult{section: &section}:
			case <-collectCtx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			sectionCh <- sectionResult{err: err}
		}
	}()

	pfErrCh := make(chan error, 1)
	go func() {
		defer close(pfErrCh)
		scanner := bufio.NewScanner(pfR)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var section EITSection
			if err := json.Unmarshal(line, &section); err != nil {
				pfErrCh <- err
				return
			}
			if !matchesExpected(&section) {
				continue
			}
			slog.Debug("upserting EIT section", "source", "eitpf", "networkId", section.OriginalNetworkID, "serviceId", section.ServiceID, "tableId", section.TableID, "sectionNumber", section.SectionNumber, "lastSectionNumber", section.LastSectionNumber, "version", section.VersionNumber, "events", len(section.Events))
			if err := programStore.UpsertPrograms(collectCtx, section.Programs()); err != nil {
				pfErrCh <- err
				return
			}
		}
		if err := scanner.Err(); err != nil {
			pfErrCh <- err
		}
	}()

	snapshot := NewSnapshot()
	finished := false
	for !finished {
		select {
		case result, ok := <-sectionCh:
			if !ok {
				finished = true
				break
			}
			if result.err != nil {
				cancel()
				_ = eitsR.Close()
				_ = pfR.Close()
				now := time.Now().UnixMilli()
				msg := result.err.Error()
				for _, key := range expected {
					_ = serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, msg)
				}
				return result.err
			}
			if result.section == nil || !matchesExpected(result.section) {
				continue
			}
			slog.Debug("observed EIT section", "source", "eits", "networkId", result.section.OriginalNetworkID, "serviceId", result.section.ServiceID, "tableId", result.section.TableID, "sectionNumber", result.section.SectionNumber, "lastSectionNumber", result.section.LastSectionNumber, "version", result.section.VersionNumber, "events", len(result.section.Events))
			snapshot.Observe(result.section, time.Now())
		case <-collectCtx.Done():
			finished = true
		}
	}
	cancel()
	_ = eitsR.Close()
	_ = pfR.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-collectErrCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Debug("EPG collector finished with error", "err", err)
			}
		case <-time.After(2 * time.Second):
		}
	}
	select {
	case err := <-pfErrCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			slog.Debug("EITPF upsert finished with error", "err", err)
		}
	case <-time.After(2 * time.Second):
	}

	now := time.Now().UnixMilli()
	var result error
	for _, key := range expected {
		if snapshot.Observed(key) {
			programs := snapshot.Programs(key)
			if !snapshot.ServiceComplete(key) {
				slog.Warn("flushing incomplete EITS collection",
					"networkId", key.NetworkID,
					"serviceId", key.ServiceID,
					"report", snapshot.CompletionReport(key))
			}
			slog.Info("merging EPG collection", "networkId", key.NetworkID, "serviceId", key.ServiceID, "programs", len(programs))
			if err := programStore.UpsertPrograms(ctx, programs); err != nil {
				_ = serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error())
				result = errors.Join(result, fmt.Errorf("service %d: merge: %w", key.ServiceID, err))
				continue
			}
			if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, now); err != nil {
				result = errors.Join(result, err)
			}
		} else {
			slog.Warn("EITS snapshot incomplete",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"report", snapshot.CompletionReport(key))
			err := fmt.Errorf("service %d EITS incomplete", key.ServiceID)
			_ = serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error())
			result = errors.Join(result, err)
		}
	}
	return result
}

func syncStoredServicePrograms(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, lister StoredProgramLister, expected []ServiceKey, retrievalTime time.Duration) error {
	syncCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	var result error
	for _, key := range expected {
		if err := syncCtx.Err(); err != nil {
			return errors.Join(result, err)
		}
		programs, err := lister.ListServicePrograms(syncCtx, key.NetworkID, key.ServiceID)
		now := time.Now().UnixMilli()
		if err != nil {
			_ = serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error())
			result = errors.Join(result, fmt.Errorf("service %d: list remote programs: %w", key.ServiceID, err))
			continue
		}
		slog.Info("syncing stored remote EPG", "networkId", key.NetworkID, "serviceId", key.ServiceID, "programs", len(programs))
		if err := programStore.ReplaceServicePrograms(ctx, key.NetworkID, key.ServiceID, 0, programs); err != nil {
			_ = serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error())
			result = errors.Join(result, fmt.Errorf("service %d: replace remote programs: %w", key.ServiceID, err))
			continue
		}
		if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, now); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}
