package epg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
	"github.com/google/uuid"
)

type Candidate struct {
	Type    string
	Channel string
}

type Network struct {
	Candidates []Candidate
	Services   []ServiceKey
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

func gatherNetwork(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, streams StreamManager, networkID uint16, candidates []Candidate, serviceKeys []ServiceKey, retrievalTime time.Duration) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGGatherNetwork,
		observability.AttrEPGNetworkID.Int(int(networkID)),
		observability.AttrEPGCandidates.Int(len(candidates)),
		observability.AttrEPGServices.Int(len(serviceKeys)),
	)
	defer func() { observability.EndSpan(span, err) }()

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
		candidateCtx, cancel := context.WithTimeout(ctx, retrievalTime)
		candidateCtx, candidateSpan := observability.StartSpan(candidateCtx, observability.SpanEPGGatherCandidate,
			observability.AttrEPGNetworkID.Int(int(networkID)),
			observability.AttrChannelType.String(candidate.Type),
			observability.AttrChannelID.String(candidate.Channel),
			observability.AttrStreamActiveSession.Bool(active[candidate]),
		)
		yes := true
		userCtx := tuner.WithUser(candidateCtx, tuner.User{
			ID: uuid.NewString(), Priority: -1, Agent: "Mahiron EPG Gatherer",
			StreamSetting: tuner.StreamSetting{
				Channel:  &config.ChannelConfig{Type: candidate.Type, Channel: candidate.Channel},
				ParseEIT: &yes,
			},
		})
		var candidateErr error
		session, candidateErr := streams.GetOrCreateWait(userCtx, candidate.Type, candidate.Channel)
		if candidateErr == nil {
			candidateErr = CollectServiceSnapshots(userCtx, programStore, serviceStore, session, serviceKeys, retrievalTime)
		}
		cancel()
		observability.EndSpan(candidateSpan, candidateErr)
		if candidateErr == nil {
			slog.Debug("finished network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("network EPG collection candidate failed", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "err", candidateErr)
		result = errors.Join(result, fmt.Errorf("%s/%s: %w", candidate.Type, candidate.Channel, candidateErr))
	}
	if result == nil {
		return fmt.Errorf("network %d has no channel candidates", networkID)
	}
	slog.Warn("network EPG collection failed", "networkId", networkID, "candidates", len(ordered), "err", result)
	return result
}

func CollectServiceSnapshots(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, session interface {
	CollectEIT(context.Context, func(*ts.EIT) error) error
}, expected []ServiceKey, retrievalTime time.Duration) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGCollectServiceSnapshots,
		observability.AttrEPGServices.Int(len(expected)),
		observability.AttrEPGRetrievalTimeMS.Int64(retrievalTime.Milliseconds()),
	)
	defer func() { observability.EndSpan(span, err) }()

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
	source := "eits"
	lister, hasStoredPrograms := session.(StoredProgramLister)
	if hasStoredPrograms {
		source = "remote"
	}
	for _, key := range expected {
		if err := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, startedAt, ""); err != nil {
			observability.RecordEPGServiceUpdateError(ctx, source, "attempt")
		}
	}
	if hasStoredPrograms {
		return syncStoredServicePrograms(ctx, programStore, serviceStore, lister, expected, retrievalTime)
	}
	collectCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	type collectionResult struct {
		collectErr error
		pfErr      error
	}
	collectDone := make(chan collectionResult, 1)

	sectionCh := make(chan *EITSection, 1)
	go func() {
		defer close(sectionCh)
		var pfErr error
		collectErr := session.CollectEIT(collectCtx, func(eit *ts.EIT) error {
			section := EITSectionFromTS(eit)
			if section == nil || !matchesExpected(section) {
				return nil
			}
			if ts.IsEITPF(section.TableID) {
				if pfErr != nil {
					return nil
				}
				slog.Debug("upserting EIT section", "source", "eitpf", "networkId", section.OriginalNetworkID, "serviceId", section.ServiceID, "tableId", section.TableID, "sectionNumber", section.SectionNumber, "lastSectionNumber", section.LastSectionNumber, "version", section.VersionNumber, "events", len(section.Events))
				sourceCtx := observability.ContextWithEPGMetricSource(collectCtx, "eitpf")
				if err := programStore.UpsertPrograms(sourceCtx, section.Programs()); err != nil {
					pfErr = err
				}
				return nil
			}
			select {
			case sectionCh <- section:
			case <-collectCtx.Done():
				return collectCtx.Err()
			}
			return nil
		})
		collectDone <- collectionResult{collectErr: collectErr, pfErr: pfErr}
	}()

	snapshot := NewSnapshot()
	finished := false
	for !finished {
		select {
		case section, ok := <-sectionCh:
			if !ok {
				finished = true
				break
			}
			if section == nil || !matchesExpected(section) {
				continue
			}
			slog.Debug("observed EIT section", "source", "eits", "networkId", section.OriginalNetworkID, "serviceId", section.ServiceID, "tableId", section.TableID, "sectionNumber", section.SectionNumber, "lastSectionNumber", section.LastSectionNumber, "version", section.VersionNumber, "events", len(section.Events))
			snapshot.Observe(section, time.Now())
		case <-collectCtx.Done():
			finished = true
		}
	}
	cancel()
	select {
	case result := <-collectDone:
		if result.collectErr != nil && !errors.Is(result.collectErr, context.Canceled) {
			slog.Debug("EPG collector finished with error", "err", result.collectErr)
		}
		if result.pfErr != nil {
			slog.Debug("EITPF upsert finished with error", "err", result.pfErr)
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
			mergeCtx, mergeSpan := observability.StartSpan(ctx, observability.SpanEPGMergeServicePrograms,
				observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
				observability.AttrEPGServiceID.Int(int(key.ServiceID)),
				observability.AttrProgramCount.Int(len(programs)),
			)
			mergeCtx = observability.ContextWithEPGMetricSource(mergeCtx, "eits")
			err := programStore.UpsertPrograms(mergeCtx, programs)
			observability.EndSpan(mergeSpan, err)
			if err != nil {
				if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
					observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
				}
				result = errors.Join(result, fmt.Errorf("service %d: merge: %w", key.ServiceID, err))
				continue
			}
			if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, now); err != nil {
				observability.RecordEPGServiceUpdateError(ctx, "eits", "success")
				result = errors.Join(result, err)
			}
		} else {
			slog.Warn("EITS snapshot incomplete",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"report", snapshot.CompletionReport(key))
			err := fmt.Errorf("service %d EITS incomplete", key.ServiceID)
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
			}
			result = errors.Join(result, err)
		}
	}
	return result
}

func syncStoredServicePrograms(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, lister StoredProgramLister, expected []ServiceKey, retrievalTime time.Duration) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGSyncStoredServicePrograms,
		observability.AttrEPGServices.Int(len(expected)),
		observability.AttrEPGRetrievalTimeMS.Int64(retrievalTime.Milliseconds()),
	)
	defer func() { observability.EndSpan(span, err) }()

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
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "remote", "attempt")
			}
			result = errors.Join(result, fmt.Errorf("service %d: list remote programs: %w", key.ServiceID, err))
			continue
		}
		slog.Info("syncing stored remote EPG", "networkId", key.NetworkID, "serviceId", key.ServiceID, "programs", len(programs))
		replaceCtx, replaceSpan := observability.StartSpan(ctx, observability.SpanEPGReplaceRemoteServicePrograms,
			observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
			observability.AttrEPGServiceID.Int(int(key.ServiceID)),
			observability.AttrProgramCount.Int(len(programs)),
		)
		replaceCtx = observability.ContextWithEPGMetricSource(replaceCtx, "remote")
		err = programStore.ReplaceServicePrograms(replaceCtx, key.NetworkID, key.ServiceID, 0, programs)
		observability.EndSpan(replaceSpan, err)
		if err != nil {
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "remote", "attempt")
			}
			result = errors.Join(result, fmt.Errorf("service %d: replace remote programs: %w", key.ServiceID, err))
			continue
		}
		if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, now); err != nil {
			observability.RecordEPGServiceUpdateError(ctx, "remote", "success")
			result = errors.Join(result, err)
		}
	}
	return result
}
