package epggather

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
)

type CollectResult struct {
	Observed     []ServiceKey
	Unobserved   []ServiceKey
	ProgramCount int
}

const eitsCollectionBuffer = 4096

var partialEITSFlushInterval = 5 * time.Second
var eitsStableStopDuration = 3 * time.Second

// eitsDeadStreamTimeout bounds how long a collection run will wait for the
// very first EIT section before giving up on the assumption the tuner
// connected but the stream is dead (e.g. an upstream lock failure that never
// closes the connection). Without this, a dead stream is only noticed after
// the full retrievalTime deadline, which can hold a tuner for minutes.
var eitsDeadStreamTimeout = 30 * time.Second

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

func (idx *expectedServiceIndex) matchesExpected(key model.ServiceKey) bool {
	byTSID, ok := idx.byNID[key.NetworkID]
	if !ok {
		return false
	}
	ids, ok := byTSID[key.StreamID]
	if !ok {
		// A zero TSID is only used by older tests and in-memory fakes. Real
		// scanned services always carry the ARIB transport_stream_id.
		ids, ok = byTSID[0]
	}
	if !ok {
		return false
	}
	_, ok = ids[key.ServiceID]
	return ok
}

func (idx *expectedServiceIndex) matchesCollectionNetwork(key model.ServiceKey) bool {
	_, ok := idx.networks[key.NetworkID]
	return ok
}

func serviceKeyFromModel(key model.ServiceKey) ServiceKey {
	return ServiceKey{NetworkID: key.NetworkID, ServiceID: key.ServiceID, TransportStreamID: key.StreamID}
}

// collectionSchedule holds the latest schedule update of each service a
// collection run heard about. The session keeps the reception state; this
// only remembers what it last reported.
type collectionSchedule struct {
	updates map[ServiceKey]model.ScheduleUpdate
	// lastProgress is when the session last reported progress.
	lastProgress time.Time
	// clock is the latest broadcast clock the session reported.
	clock int64
}

func newCollectionSchedule() *collectionSchedule {
	return &collectionSchedule{updates: make(map[ServiceKey]model.ScheduleUpdate)}
}

func (c *collectionSchedule) observe(update model.ScheduleUpdate) ServiceKey {
	key := serviceKeyFromModel(update.Service)
	c.updates[key] = update
	c.lastProgress = time.Now()
	c.clock = max(c.clock, update.ObservedAt)
	return key
}

func (c *collectionSchedule) nowMillis() int64 {
	if c.clock != 0 {
		return c.clock
	}
	return time.Now().UnixMilli()
}

func (c *collectionSchedule) stableFor(duration time.Duration) bool {
	return !c.lastProgress.IsZero() && time.Since(c.lastProgress) >= duration
}

// observed reports whether the service's basic tables arrived.
func (c *collectionSchedule) observed(key ServiceKey) bool {
	return c.updates[key].BasicObserved
}

func (c *collectionSchedule) events(key ServiceKey) []model.Event {
	update, ok := c.updates[key]
	if !ok || update.Events == nil {
		return nil
	}
	return update.Events()
}

func (c *collectionSchedule) diagnosis(key ServiceKey) string {
	update, ok := c.updates[key]
	if !ok || update.Diagnosis == nil {
		return ""
	}
	return update.Diagnosis()
}

// CollectSchedule collects the EIT of one channel until the expected
// services are complete, the stream turns out dead or retrievalTime passes.
type CollectSchedule = func(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error

func CollectServiceSnapshots(ctx context.Context, events EventWriter, serviceStore ServiceStore, collect CollectSchedule, expected []ServiceKey, retrievalTime time.Duration) (result *CollectResult, err error) {
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

	startedAt := time.Now().UnixMilli()
	for _, key := range expected {
		if err := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, startedAt, ""); err != nil {
			observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
		}
	}
	collectCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	type collectionResult struct {
		collectErr error
	}
	collectDone := make(chan collectionResult, 1)

	updateCh := make(chan model.ScheduleUpdate, eitsCollectionBuffer)
	pfCh := make(chan model.PresentFollowing, eitsCollectionBuffer)
	go func() {
		onSchedule := func(update model.ScheduleUpdate) error {
			if !index.matchesCollectionNetwork(update.Service) {
				return nil
			}
			select {
			case updateCh <- update:
			case <-collectCtx.Done():
				return collectCtx.Err()
			}
			return nil
		}
		onPresentFollowing := func(pf model.PresentFollowing) error {
			if !index.matchesExpected(pf.Service) {
				return nil
			}
			select {
			case pfCh <- pf:
			case <-collectCtx.Done():
				return collectCtx.Err()
			}
			return nil
		}
		collectDone <- collectionResult{collectErr: collect(collectCtx, onSchedule, onPresentFollowing)}
	}()

	schedule := newCollectionSchedule()
	pfUpserts := newEITPFUpserter(collectCtx, events)
	defer pfUpserts.wait()
	defer pfUpserts.stop()
	partialFlushes := newPartialEITSFlusher(collectCtx, events)
	defer partialFlushes.wait()
	defer partialFlushes.stop()
	flushTicker := time.NewTicker(partialEITSFlushInterval)
	defer flushTicker.Stop()
	deadStreamTimer := time.NewTimer(eitsDeadStreamTimeout)
	defer deadStreamTimer.Stop()
	dirtyServices := make(map[ServiceKey]struct{})
	observedServices := make(map[ServiceKey]struct{})
	var pfUpdates, scheduleUpdates, received int
	handleUpdate := func(update model.ScheduleUpdate) {
		scheduleUpdates++
		key := schedule.observe(update)
		dirtyServices[key] = struct{}{}
		observedServices[key] = struct{}{}
	}
	handlePresentFollowing := func(pf model.PresentFollowing) {
		pfUpdates++
		var current []model.Event
		for _, event := range []*model.Event{pf.Present, pf.Following} {
			if event != nil {
				current = append(current, *event)
			}
		}
		pfUpserts.enqueue(current)
	}
	finished := false
	var collectorResult collectionResult
	collectorDone := false
	for !finished {
		select {
		case update := <-updateCh:
			received++
			handleUpdate(update)
			if shouldStopEITSCollection(schedule, expected) && schedule.stableFor(eitsStableStopDuration) {
				cancel()
			}
		case pf := <-pfCh:
			received++
			handlePresentFollowing(pf)
		case <-flushTicker.C:
			if partialFlushes.flush(schedule, dirtyServices) {
				dirtyServices = make(map[ServiceKey]struct{})
			}
			if shouldStopEITSCollection(schedule, expected) && schedule.stableFor(eitsStableStopDuration) {
				cancel()
			}
		case <-deadStreamTimer.C:
			if received == 0 {
				slog.Warn("aborting EPG collection: no EIT sections received, tuner may be connected to a dead stream",
					"expectedServices", len(expected),
					"waited", eitsDeadStreamTimeout)
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
	// The collector may have buffered updates that the loop never got to
	// before collectDone or the deadline won the select. Drain them so
	// already-reported progress is not dropped.
	for drained := false; !drained; {
		select {
		case update := <-updateCh:
			handleUpdate(update)
		case pf := <-pfCh:
			handlePresentFollowing(pf)
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
			slog.Warn("EPG collector finished with error", "err", collectorResult.collectErr)
		}
		if pfErr := pfUpserts.Err(); pfErr != nil {
			slog.Debug("EITPF upsert finished with error", "err", pfErr)
		}
	}
	slog.Debug("EPG collection updates observed",
		"eitpfUpdates", pfUpdates,
		"eitsUpdates", scheduleUpdates,
		"observedServices", len(observedServices),
		"expectedServices", len(expected))

	collectErr := persistObservedSnapshots(ctx, events, serviceStore, schedule, expected, observedServices, schedule.nowMillis(), result)
	return result, collectErr
}

// persistObservedSnapshots stores the observed schedules, records
// per-service EPG attempt/success state, and populates result.Observed /
// result.Unobserved. It returns the joined error of the persist stage.
func persistObservedSnapshots(ctx context.Context, writer EventWriter, serviceStore ServiceStore, schedule *collectionSchedule, expected []ServiceKey, observedServices map[ServiceKey]struct{}, updatedAt int64, result *CollectResult) error {
	var collectErr error
	observed := 0
	expectedObserved := 0
	var unobserved error
	observedEvents := make(map[ServiceKey][]model.Event)
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
		if _, ok := expectedSeen[key]; ok || !schedule.observed(key) {
			continue
		}
		if expectedTSID, ok := expectedTransportStreams[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}]; ok && expectedTSID != 0 && key.TransportStreamID != expectedTSID {
			continue
		}
		mergeKeys = append(mergeKeys, key)
	}
	var shared [][]model.Event
	for _, key := range mergeKeys {
		if !schedule.observed(key) {
			continue
		}
		observedEvents[key] = schedule.events(key)
		shared = append(shared, observedEvents[key])
	}
	fillEventsFromSharedPeers(shared...)
	for _, key := range mergeKeys {
		_, isExpected := expectedSeen[key]
		if schedule.observed(key) {
			result.Observed = append(result.Observed, key)
			if isExpected {
				expectedObserved++
			}
			observed++
			events := observedEvents[key]
			result.ProgramCount += len(events)
			update := schedule.updates[key]
			diagnosis := schedule.diagnosis(key)
			missingTitles, titleTotal := eventTitleCounts(events)
			if !update.BasicComplete {
				slog.Warn("flushing incomplete EITS collection",
					"networkId", key.NetworkID,
					"serviceId", key.ServiceID,
					"report", diagnosis)
			}
			slog.Info("finished EITS collection",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"programs", len(events),
				"missingTitles", missingTitles,
				"titleTotal", titleTotal,
				"basicComplete", update.BasicComplete,
				"observedExtendedComplete", update.ExtendedComplete,
				"report", diagnosis)
			mergeCtx, mergeSpan := observability.StartSpan(ctx, observability.SpanEPGMergeServicePrograms,
				observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
				observability.AttrEPGServiceID.Int(int(key.ServiceID)),
				observability.AttrProgramCount.Int(len(events)),
			)
			mergeCtx = observability.ContextWithEPGMetricSource(mergeCtx, "eits")
			err := writer.UpsertEvents(mergeCtx, events)
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
			if warning := lowQualityEventWarning(events); warning != "" {
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
				"report", schedule.diagnosis(key))
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

// shouldStopEITSCollection reports whether every expected service's basic
// and extended tables are complete and the collected events are good enough.
// A service without extended tables counts as extended-complete.
func shouldStopEITSCollection(schedule *collectionSchedule, expected []ServiceKey) bool {
	if len(expected) == 0 {
		return false
	}
	var events [][]model.Event
	for _, key := range expected {
		update, ok := schedule.updates[key]
		if !ok || !update.BasicComplete || !update.ExtendedComplete {
			return false
		}
		events = append(events, schedule.events(key))
	}
	fillEventsFromSharedPeers(events...)
	var all []model.Event
	for _, group := range events {
		all = append(all, group...)
	}
	return lowQualityEventWarning(all) == ""
}
