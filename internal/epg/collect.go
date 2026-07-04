package epg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

type CollectResult struct {
	Observed     []ServiceKey
	Unobserved   []ServiceKey
	ProgramCount int
}

type eitClockCollector interface {
	CollectEITWithClock(context.Context, func(*ts.EIT, time.Time) error) error
}

const eitsCollectionBuffer = 4096

var partialEITSFlushInterval = 5 * time.Second
var eitsStableStopDuration = 3 * time.Second

// expectedServiceIndex answers membership queries for the services a collection
// run is targeting, keyed by original network / transport stream / service ID.
type expectedServiceIndex struct {
	byNID    map[uint16]map[uint16]map[uint16]struct{}
	networks map[uint16]struct{}
}

func newExpectedServiceIndex(expected []ServiceKey) *expectedServiceIndex {
	idx := &expectedServiceIndex{
		byNID:    make(map[uint16]map[uint16]map[uint16]struct{}, len(expected)),
		networks: make(map[uint16]struct{}, len(expected)),
	}
	for _, key := range expected {
		idx.networks[key.NetworkID] = struct{}{}
		if idx.byNID[key.NetworkID] == nil {
			idx.byNID[key.NetworkID] = make(map[uint16]map[uint16]struct{})
		}
		if idx.byNID[key.NetworkID][key.TransportStreamID] == nil {
			idx.byNID[key.NetworkID][key.TransportStreamID] = make(map[uint16]struct{})
		}
		idx.byNID[key.NetworkID][key.TransportStreamID][key.ServiceID] = struct{}{}
	}
	return idx
}

func (idx *expectedServiceIndex) matchesExpected(section *EITSection) bool {
	byTSID, ok := idx.byNID[section.OriginalNetworkID]
	if !ok {
		return false
	}
	ids, ok := byTSID[section.TransportStreamID]
	if !ok {
		// A zero TSID is only used by older tests and in-memory fakes. Real
		// scanned services always carry the ARIB transport_stream_id.
		ids, ok = byTSID[0]
	}
	if !ok {
		return false
	}
	_, ok = ids[section.ServiceID]
	return ok
}

func (idx *expectedServiceIndex) matchesCollectionNetwork(section *EITSection) bool {
	_, ok := idx.networks[section.OriginalNetworkID]
	return ok
}

// collectClock tracks the most recent broadcast clock observed on the stream,
// falling back to wall-clock time until one is seen. It is safe for concurrent
// use by the collector goroutine and the collection loop.
type collectClock struct {
	mu     sync.Mutex
	latest time.Time
}

func (c *collectClock) set(clock time.Time) {
	if clock.IsZero() {
		return
	}
	c.mu.Lock()
	c.latest = clock
	c.mu.Unlock()
}

func (c *collectClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.latest.IsZero() {
		return c.latest
	}
	return time.Now()
}

func (c *collectClock) nowMillis() int64 {
	return c.now().UnixMilli()
}

func CollectServiceSnapshots(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, session interface {
	CollectEIT(context.Context, func(*ts.EIT) error) error
}, expected []ServiceKey, retrievalTime time.Duration) (result *CollectResult, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGCollectServiceSnapshots,
		observability.AttrEPGServices.Int(len(expected)),
		observability.AttrEPGRetrievalTimeMS.Int64(retrievalTime.Milliseconds()),
	)
	defer func() { observability.EndSpan(span, err) }()

	if len(expected) == 0 {
		return nil, errors.New("collectServiceSnapshots: expected is empty")
	}
	result = &CollectResult{}
	index := newExpectedServiceIndex(expected)
	clock := &collectClock{}

	startedAt := clock.nowMillis()
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
		err := syncStoredServicePrograms(ctx, programStore, serviceStore, lister, expected, retrievalTime)
		if err == nil {
			result.Observed = append(result.Observed, expected...)
		}
		return result, err
	}
	collectCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	type collectionResult struct {
		collectErr error
	}
	collectDone := make(chan collectionResult, 1)

	sectionCh := make(chan *EITSection, eitsCollectionBuffer)
	go func() {
		observeEIT := func(eit *ts.EIT, sectionClock time.Time) error {
			clock.set(sectionClock)
			section := EITSectionFromTS(eit)
			if section == nil || !index.matchesCollectionNetwork(section) {
				return nil
			}
			select {
			case sectionCh <- section:
			case <-collectCtx.Done():
				return collectCtx.Err()
			}
			return nil
		}
		var collectErr error
		if collector, ok := session.(eitClockCollector); ok {
			collectErr = collector.CollectEITWithClock(collectCtx, observeEIT)
		} else {
			collectErr = session.CollectEIT(collectCtx, func(eit *ts.EIT) error {
				return observeEIT(eit, time.Time{})
			})
		}
		collectDone <- collectionResult{collectErr: collectErr}
	}()

	snapshot := NewSnapshot()
	pfUpserts := newEITPFUpserter(collectCtx, programStore)
	defer pfUpserts.wait()
	defer pfUpserts.stop()
	partialFlushes := newPartialEITSFlusher(collectCtx, programStore)
	defer partialFlushes.wait()
	defer partialFlushes.stop()
	flushTicker := time.NewTicker(partialEITSFlushInterval)
	defer flushTicker.Stop()
	dirtyServices := make(map[ServiceKey]struct{})
	observedServices := make(map[ServiceKey]struct{})
	var eitpfSections int
	var eitsSections int
	var ignoredSections int
	handleSection := func(section *EITSection) {
		if section == nil || !index.matchesCollectionNetwork(section) {
			ignoredSections++
			return
		}
		if ts.IsEITPF(section.TableID) {
			if !index.matchesExpected(section) {
				ignoredSections++
				return
			}
			eitpfSections++
			pfUpserts.enqueue(section.Programs())
			return
		}
		eitsSections++
		snapshot.Observe(section, clock.now())
		key := ServiceKey{NetworkID: section.OriginalNetworkID, ServiceID: section.ServiceID, TransportStreamID: section.TransportStreamID}
		dirtyServices[key] = struct{}{}
		observedServices[key] = struct{}{}
	}
	finished := false
	var collectorResult collectionResult
	collectorDone := false
	for !finished {
		select {
		case section := <-sectionCh:
			handleSection(section)
			if shouldStopEITSCollection(snapshot, expected) && snapshot.StableFor(clock.now(), eitsStableStopDuration) {
				cancel()
			}
		case <-flushTicker.C:
			if partialFlushes.flush(snapshot, dirtyServices) {
				dirtyServices = make(map[ServiceKey]struct{})
			}
			if shouldStopEITSCollection(snapshot, expected) && snapshot.StableFor(clock.now(), eitsStableStopDuration) {
				cancel()
			}
		case collectorResult = <-collectDone:
			collectorDone = true
			finished = true
			cancel()
		case <-collectCtx.Done():
			finished = true
		}
	}
	cancel()
	// The collector may have buffered sections in sectionCh that the loop never
	// got to before collectDone or the deadline won the select. Drain them so
	// already-observed sections are not dropped from the snapshot.
	for drained := false; !drained; {
		select {
		case section := <-sectionCh:
			handleSection(section)
		default:
			drained = true
		}
	}
	pfUpserts.stop()
	pfUpserts.wait()
	partialFlushes.stop()
	partialFlushes.wait()
	if !collectorDone {
		if ctx.Err() != nil {
			slog.Debug("skipping EPG collector drain during shutdown", "err", ctx.Err())
		} else {
			select {
			case collectorResult = <-collectDone:
				collectorDone = true
			case <-time.After(2 * time.Second):
			}
		}
	}
	if collectorDone {
		if collectorResult.collectErr != nil && !errors.Is(collectorResult.collectErr, context.Canceled) {
			slog.Debug("EPG collector finished with error", "err", collectorResult.collectErr)
		}
		if pfErr := pfUpserts.Err(); pfErr != nil {
			slog.Debug("EITPF upsert finished with error", "err", pfErr)
		}
	}
	slog.Debug("EPG collection sections observed",
		"eitpfSections", eitpfSections,
		"eitsSections", eitsSections,
		"ignoredSections", ignoredSections,
		"observedServices", len(observedServices),
		"expectedServices", len(expected))

	collectErr := persistObservedSnapshots(ctx, programStore, serviceStore, snapshot, expected, observedServices, clock.nowMillis(), result)
	return result, collectErr
}

// persistObservedSnapshots merges the observed snapshot into the program store,
// records per-service EPG attempt/success state, and populates result.Observed /
// result.Unobserved. It returns the joined error of the merge/persist stage.
func persistObservedSnapshots(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, snapshot *Snapshot, expected []ServiceKey, observedServices map[ServiceKey]struct{}, updatedAt int64, result *CollectResult) error {
	var collectErr error
	observed := 0
	expectedObserved := 0
	var unobserved error
	observedPrograms := make(map[ServiceKey][]*program.Program)
	var allObservedPrograms []*program.Program
	mergeKeys := append([]ServiceKey(nil), expected...)
	expectedSeen := make(map[ServiceKey]struct{}, len(expected))
	type serviceIdentity struct {
		networkID uint16
		serviceID uint16
	}
	expectedTransportStreams := make(map[serviceIdentity]uint16, len(expected))
	for _, key := range expected {
		expectedSeen[key] = struct{}{}
		expectedTransportStreams[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}] = key.TransportStreamID
	}
	for key := range observedServices {
		if _, ok := expectedSeen[key]; ok || !snapshot.Observed(key) {
			continue
		}
		if expectedTSID, ok := expectedTransportStreams[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}]; ok && expectedTSID != 0 && key.TransportStreamID != expectedTSID {
			continue
		}
		mergeKeys = append(mergeKeys, key)
	}
	for _, key := range mergeKeys {
		if !snapshot.Observed(key) {
			continue
		}
		programs := snapshot.Programs(key)
		observedPrograms[key] = programs
		allObservedPrograms = append(allObservedPrograms, programs...)
	}
	fillProgramsFromSharedPeers(allObservedPrograms)
	for _, key := range mergeKeys {
		_, isExpected := expectedSeen[key]
		if snapshot.Observed(key) {
			result.Observed = append(result.Observed, key)
			if isExpected {
				expectedObserved++
			}
			observed++
			programs := observedPrograms[key]
			result.ProgramCount += len(programs)
			report := snapshot.CompletionReport(key)
			basicComplete := snapshot.ServiceComplete(key)
			observedExtendedComplete := snapshot.ObservedExtendedReady([]ServiceKey{key})
			missingTitles, titleTotal := programTitleCounts(programs)
			if !basicComplete {
				slog.Warn("flushing incomplete EITS collection",
					"networkId", key.NetworkID,
					"serviceId", key.ServiceID,
					"report", report)
			}
			slog.Info("finished EITS collection",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"programs", len(programs),
				"missingTitles", missingTitles,
				"titleTotal", titleTotal,
				"basicComplete", basicComplete,
				"observedExtendedComplete", observedExtendedComplete,
				"report", report)
			mergeCtx, mergeSpan := observability.StartSpan(ctx, observability.SpanEPGMergeServicePrograms,
				observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
				observability.AttrEPGServiceID.Int(int(key.ServiceID)),
				observability.AttrProgramCount.Int(len(programs)),
			)
			mergeCtx = observability.ContextWithEPGMetricSource(mergeCtx, "eits")
			err := programStore.UpsertPrograms(mergeCtx, programs)
			observability.EndSpan(mergeSpan, err)
			if err != nil {
				if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, err.Error()); attemptErr != nil {
					observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
				}
				collectErr = errors.Join(collectErr, fmt.Errorf("service %d: merge: %w", key.ServiceID, err))
				continue
			}
			if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, updatedAt); err != nil {
				observability.RecordEPGServiceUpdateError(ctx, "eits", "success")
				collectErr = errors.Join(collectErr, err)
			}
			if warning := lowQualityProgramWarning(programs); warning != "" {
				slog.Warn("EITS collection quality is low", "networkId", key.NetworkID, "serviceId", key.ServiceID, "warning", warning)
				if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, warning); attemptErr != nil {
					observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
				}
			}
		} else if isExpected {
			result.Unobserved = append(result.Unobserved, key)
			slog.Warn("EITS snapshot incomplete",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"report", snapshot.CompletionReport(key))
			err := fmt.Errorf("service %d EITS incomplete", key.ServiceID)
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
			}
			unobserved = errors.Join(unobserved, err)
		}
	}
	if expectedObserved == 0 {
		collectErr = errors.Join(collectErr, unobserved)
	}
	return collectErr
}

func shouldStopEITSCollection(snapshot *Snapshot, expected []ServiceKey) bool {
	if snapshot == nil || !snapshot.AllReady(expected) {
		return false
	}
	if !snapshot.ObservedExtendedReady(expected) {
		return false
	}
	var programs []*program.Program
	for _, key := range expected {
		programs = append(programs, snapshot.Programs(key)...)
	}
	fillProgramsFromSharedPeers(programs)
	return lowQualityProgramWarning(programs) == ""
}
