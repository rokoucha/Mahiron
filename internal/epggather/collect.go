package epggather

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
	"go.opentelemetry.io/otel/attribute"
)

type CollectResult struct {
	Observed     []model.ServiceKey
	Unobserved   []model.ServiceKey
	ProgramCount int
	Incomplete   []model.ServiceKey
	EndReason    string
	ReceiveError string
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
	byNID map[uint16]map[uint16]map[uint16]struct{}
}

func newExpectedServiceIndex(expected []model.ServiceKey) *expectedServiceIndex {
	idx := &expectedServiceIndex{
		byNID: make(map[uint16]map[uint16]map[uint16]struct{}, len(expected)),
	}
	for _, key := range expected {
		if idx.byNID[key.NetworkID] == nil {
			idx.byNID[key.NetworkID] = make(map[uint16]map[uint16]struct{})
		}
		if idx.byNID[key.NetworkID][key.StreamID] == nil {
			idx.byNID[key.NetworkID][key.StreamID] = make(map[uint16]struct{})
		}
		idx.byNID[key.NetworkID][key.StreamID][key.ServiceID] = struct{}{}
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
		// A zero stream ID means the scan did not learn it, as with a remote
		// that omits transportStreamId.
		ids, ok = byTSID[0]
	}
	if !ok {
		return false
	}
	_, ok = ids[key.ServiceID]
	return ok
}

func (idx *expectedServiceIndex) matchesCollectionNetwork(key model.ServiceKey) bool {
	_, ok := idx.byNID[key.NetworkID]
	return ok
}

func (idx *expectedServiceIndex) matchesCollectionStream(key model.ServiceKey) bool {
	streams := idx.byNID[key.NetworkID]
	_, known := streams[key.StreamID]
	_, unknown := streams[0]
	return known || unknown && idx.matchesExpected(key)
}

// collectionSchedule holds the latest schedule update of each service a
// collection run heard about. The session keeps the reception state; this
// only remembers what it last reported.
type collectionSchedule struct {
	updates map[model.ServiceKey]model.ScheduleUpdate
	// unknownStream maps services whose expected key has no stream ID (a
	// remote that omits transportStreamId) to that key, so the real stream
	// ID the session reports is filed under it.
	unknownStream map[serviceIdentity]model.ServiceKey
	// lastProgress is when the session last reported progress.
	lastProgress time.Time
	// clock is the latest broadcast clock the session reported.
	clock int64
}

type serviceIdentity struct {
	networkID uint16
	serviceID uint16
}

func newCollectionSchedule(expected []model.ServiceKey) *collectionSchedule {
	c := &collectionSchedule{updates: make(map[model.ServiceKey]model.ScheduleUpdate)}
	for _, key := range expected {
		if key.StreamID != 0 {
			continue
		}
		if c.unknownStream == nil {
			c.unknownStream = make(map[serviceIdentity]model.ServiceKey)
		}
		c.unknownStream[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}] = key
	}
	return c
}

func (c *collectionSchedule) observe(update model.ScheduleUpdate, progress bool) model.ServiceKey {
	key := update.Service
	if expected, ok := c.unknownStream[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}]; ok {
		key = expected
	}
	c.updates[key] = update
	if progress {
		c.lastProgress = time.Now()
	}
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
func (c *collectionSchedule) observed(key model.ServiceKey) bool {
	return c.updates[key].BasicObserved
}

func (c *collectionSchedule) events(key model.ServiceKey) []model.Event {
	update, ok := c.updates[key]
	if !ok || update.Events == nil {
		return nil
	}
	return update.Events()
}

func (c *collectionSchedule) diagnosis(key model.ServiceKey) string {
	update, ok := c.updates[key]
	if !ok {
		return "no schedule sections received"
	}
	if !update.BasicObserved {
		return "no basic schedule received"
	}
	if update.Diagnosis == nil {
		return "diagnosis unavailable"
	}
	return update.Diagnosis()
}

func (c *collectionSchedule) snapshot(key model.ServiceKey, events []model.Event) model.ScheduleSnapshot {
	snapshot := model.ScheduleSnapshot{Service: c.updates[key].Service, Events: events}
	if revisions := c.updates[key].Revisions; revisions != nil {
		snapshot.Revisions = revisions()
	}
	return snapshot
}

// CollectSchedule collects the EIT of one channel until the expected
// services are complete, the stream turns out dead or retrievalTime passes.
type CollectSchedule = func(context.Context, func(model.ScheduleUpdate) error, func(model.PresentFollowing) error) error

func CollectServiceSnapshots(ctx context.Context, events ScheduleWriter, serviceStore ServiceStore, collect CollectSchedule, expected []model.ServiceKey, retrievalTime time.Duration) (result *CollectResult, err error) {
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
	telemetry := collectionTrace{span: span, started: time.Now()}

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

	schedule := newCollectionSchedule(expected)
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
	stableTimer := time.NewTimer(time.Hour)
	stableTimer.Stop()
	defer stableTimer.Stop()
	var stableC <-chan time.Time
	dirtyServices := make(map[model.ServiceKey]struct{})
	observedServices := make(map[model.ServiceKey]struct{})
	var pfUpdates, scheduleUpdates, received int
	handleUpdate := func(update model.ScheduleUpdate) {
		scheduleUpdates++
		key := update.Service
		if mapped, ok := schedule.unknownStream[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}]; ok {
			key = mapped
		}
		previous := schedule.updates[key]
		target := index.matchesExpected(update.Service)
		key = schedule.observe(update, target)
		telemetry.observe(update, previous, target)
		if target {
			stableTimer.Stop()
			stableC = nil
			if targetTablesComplete(schedule, expected) {
				stableTimer.Reset(eitsStableStopDuration)
				stableC = stableTimer.C
			}
		}
		dirtyServices[key] = struct{}{}
		observedServices[key] = struct{}{}
	}
	handlePresentFollowing := func(pf model.PresentFollowing) {
		pfUpdates++
		telemetry.firstReceive("present_following")
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
	endReason := "deadline"
	for !finished {
		select {
		case update := <-updateCh:
			received++
			handleUpdate(update)
		case pf := <-pfCh:
			received++
			handlePresentFollowing(pf)
		case <-flushTicker.C:
			if partialFlushes.flush(schedule, dirtyServices) {
				dirtyServices = make(map[model.ServiceKey]struct{})
			}
		case <-stableC:
			stableC = nil
			for pending := len(updateCh); pending > 0; pending-- {
				handleUpdate(<-updateCh)
			}
			if schedule.stableFor(eitsStableStopDuration) && shouldStopEITSCollection(schedule, expected) {
				endReason = "complete"
				finished = true
			}
		case <-deadStreamTimer.C:
			if received == 0 {
				slog.Warn("aborting EPG collection: no EIT sections received, tuner may be connected to a dead stream",
					"expectedServices", len(expected),
					"waited", eitsDeadStreamTimeout)
				endReason = "no_eit"
				finished = true
			}
		case collectorResult = <-collectDone:
			collectorDone = true
			endReason = "stream_ended"
			if collectorResult.collectErr != nil {
				endReason = "receive_error"
			}
			if collectCtx.Err() != nil {
				endReason = "deadline"
			}
			if ctx.Err() != nil {
				endReason = "canceled"
			}
			finished = true
			cancel()
		case <-collectCtx.Done():
			if ctx.Err() != nil {
				endReason = "canceled"
			}
			finished = true
		}
	}
	cancel()
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
	var receiveErr error
	if collectorDone {
		expectedCancellation := (errors.Is(collectorResult.collectErr, context.Canceled) || errors.Is(collectorResult.collectErr, context.DeadlineExceeded)) && endReason != "receive_error"
		if collectorResult.collectErr != nil && !expectedCancellation {
			receiveErr = collectorResult.collectErr
			endReason = "receive_error"
			slog.Warn("EPG reception failed", "err", receiveErr)
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

	persistCtx := ctx
	if ctx.Err() != nil {
		var persistCancel context.CancelFunc
		persistCtx, persistCancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer persistCancel()
	}
	collectErr := persistObservedSnapshots(persistCtx, events, serviceStore, schedule, expected, observedServices, schedule.nowMillis(), result, receiveErr)
	if ctx.Err() != nil {
		endReason = "canceled"
	}
	result.EndReason = endReason
	if receiveErr != nil {
		result.ReceiveError = receiveErr.Error()
	}
	telemetry.finish(schedule, expected, endReason, result, receiveErr)
	span.SetAttributes(attribute.Int("epg.schedule_updates", scheduleUpdates), attribute.Int("epg.present_following_updates", pfUpdates))
	slog.Info("EPG collection finished", "reason", endReason, "targetServices", len(expected), "incompleteTargets", len(result.Incomplete), "programs", result.ProgramCount)
	return result, errors.Join(collectErr, ctx.Err())
}

// persistObservedSnapshots stores the observed schedules, records
// per-service EPG attempt/success state, and populates result.Observed /
// result.Unobserved. It returns the joined error of the persist stage.
func persistObservedSnapshots(ctx context.Context, writer ScheduleWriter, serviceStore ServiceStore, schedule *collectionSchedule, expected []model.ServiceKey, observedServices map[model.ServiceKey]struct{}, updatedAt int64, result *CollectResult, receiveErr error) error {
	var collectErr error
	index := newExpectedServiceIndex(expected)
	expectedObserved := 0
	var unobserved error
	observedEvents := make(map[model.ServiceKey][]model.Event)
	mergeKeys := append([]model.ServiceKey(nil), expected...)
	expectedSeen := make(map[model.ServiceKey]struct{}, len(expected))
	expectedTransportStreams := make(map[serviceIdentity]uint16, len(expected))
	for _, key := range expected {
		expectedSeen[key] = struct{}{}
		expectedTransportStreams[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}] = key.StreamID
	}
	for key := range observedServices {
		if _, ok := expectedSeen[key]; ok || !schedule.observed(key) {
			continue
		}
		if expectedTSID, ok := expectedTransportStreams[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}]; ok && expectedTSID != 0 && key.StreamID != expectedTSID {
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
			inTargetStream := index.matchesCollectionStream(key)

			if isExpected {
				expectedObserved++
			}
			events := observedEvents[key]
			result.ProgramCount += len(events)
			update := schedule.updates[key]
			diagnosis := ""
			incomplete := !update.BasicComplete || !update.ExtendedComplete
			if isExpected && incomplete {
				diagnosis = schedule.diagnosis(key)
				result.Incomplete = append(result.Incomplete, key)
				slog.Warn("target EPG collection incomplete", "networkId", key.NetworkID, "serviceId", key.ServiceID, "basicComplete", update.BasicComplete, "extendedComplete", update.ExtendedComplete, "report", diagnosis)
			}
			mergeCtx, mergeSpan := observability.StartSpan(ctx, observability.SpanEPGMergeServicePrograms,
				observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
				observability.AttrEPGServiceID.Int(int(key.ServiceID)),
				observability.AttrProgramCount.Int(len(events)),
				attribute.Bool("epg.target", isExpected), attribute.Bool("epg.basic_complete", update.BasicComplete), attribute.Bool("epg.extended_complete", update.ExtendedComplete),
			)
			mergeCtx = observability.ContextWithEPGMetricSource(mergeCtx, "eits")
			err := writer.UpsertSchedule(mergeCtx, schedule.snapshot(key, events))
			observability.EndSpan(mergeSpan, err)
			if err != nil {
				if inTargetStream {
					if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, err.Error()); attemptErr != nil {
						observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
					}
				}
				collectErr = errors.Join(collectErr, fmt.Errorf("service %d: merge: %w", key.ServiceID, err))
				continue
			}
			if inTargetStream && (isExpected || !incomplete) {
				result.Observed = append(result.Observed, key)
			}
			// Opportunistic partial data must not make a future target look fresh.
			if !inTargetStream {
				continue
			}
			if incomplete {
				warning := "EPG reception incomplete: " + schedule.diagnosis(key)
				if quality := lowQualityEventWarning(events); quality != "" {
					warning += "; " + quality
				}
				if receiveErr != nil {
					warning += ": " + receiveErr.Error()
				}
				if err := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, warning); err != nil {
					collectErr = errors.Join(collectErr, err)
				}
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
			result.Incomplete = append(result.Incomplete, key)
			slog.Warn("target EPG not received",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"report", schedule.diagnosis(key))
			err := fmt.Errorf("service %d EITS incomplete", key.ServiceID)
			if receiveErr != nil {
				err = fmt.Errorf("service %d EPG reception failed: %w", key.ServiceID, receiveErr)
			}
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

func targetTablesComplete(schedule *collectionSchedule, expected []model.ServiceKey) bool {
	if len(expected) == 0 {
		return false
	}
	for _, key := range expected {
		u := schedule.updates[key]
		if !u.BasicComplete || !u.ExtendedComplete {
			return false
		}
	}
	return true
}

// shouldStopEITSCollection also checks quality after reception has stabilized.
func shouldStopEITSCollection(schedule *collectionSchedule, expected []model.ServiceKey) bool {
	if !targetTablesComplete(schedule, expected) {
		return false
	}
	var events [][]model.Event
	for _, key := range expected {
		events = append(events, schedule.events(key))
	}
	fillEventsFromSharedPeers(events...)
	var all []model.Event
	for _, group := range events {
		all = append(all, group...)
	}
	return lowQualityEventWarning(all) == ""
}
