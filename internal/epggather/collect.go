package epggather

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/tuner"
	"go.opentelemetry.io/otel/attribute"
)

type CollectResult struct {
	Observed         []model.ServiceKey
	Unobserved       []model.ServiceKey
	ProgramCount     int
	Incomplete       []model.ServiceKey
	EndReason        string
	ReceiveError     string
	DeferredExtended []model.ServiceKey
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

type collectionResult struct{ collectErr error }

// collectionRun owns one live collection's state. Callbacks only send to its
// channels; the receiving goroutine alone updates schedules and timers.
type collectionRun struct {
	events       ScheduleWriter
	serviceStore ServiceStore

	expected  []model.ServiceKey
	option    CollectOptions
	result    *CollectResult
	index     *expectedServiceIndex
	telemetry collectionTrace
	schedule  *collectionSchedule

	pfUpserts                               *eitPFUpserter
	partialFlushes                          *partialEITSFlusher
	flushTicker                             *time.Ticker
	deadStreamTimer, stableTimer, idleTimer *time.Timer
	stableC, idleC                          <-chan time.Time

	dirtyServices, observedServices      map[model.ServiceKey]struct{}
	pfUpdates, scheduleUpdates, received int

	collectDone     chan collectionResult
	updateCh        chan model.ScheduleUpdate
	pfCh            chan model.PresentFollowing
	collectorResult collectionResult
	collectorDone   bool
	endReason       string
}

func CollectServiceSnapshots(ctx context.Context, events ScheduleWriter, serviceStore ServiceStore, collect CollectSchedule, expected []model.ServiceKey, retrievalTime time.Duration, options ...CollectOptions) (result *CollectResult, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGCollectServiceSnapshots,
		observability.AttrEPGServices.Int(len(expected)),
		observability.AttrEPGRetrievalTimeMS.Int64(retrievalTime.Milliseconds()),
	)
	defer func() { observability.EndSpan(span, err) }()

	if len(expected) == 0 {
		return nil, errors.New("collectServiceSnapshots: expected is empty")
	}
	var option CollectOptions
	if len(options) > 0 {
		option = options[0]
	}
	c := &collectionRun{events: events, serviceStore: serviceStore, expected: expected, option: option,
		result: &CollectResult{}, index: newExpectedServiceIndex(expected), telemetry: collectionTrace{span: span, started: time.Now()},
		schedule: newCollectionSchedule(expected), dirtyServices: make(map[model.ServiceKey]struct{}), observedServices: make(map[model.ServiceKey]struct{}), endReason: "deadline"}
	return c.run(ctx, collect, retrievalTime)
}

func (c *collectionRun) run(ctx context.Context, collect CollectSchedule, retrievalTime time.Duration) (*CollectResult, error) {
	startedAt := time.Now().UnixMilli()
	for _, key := range c.expected {
		if err := c.serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, startedAt, ""); err != nil {
			observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
		}
	}
	collectCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	c.startCollector(collectCtx, collect)
	c.startWorkers(collectCtx)
	defer c.stopWorkers()
	defer c.flushTicker.Stop()
	defer c.deadStreamTimer.Stop()
	defer c.stableTimer.Stop()
	defer c.idleTimer.Stop()
	c.receive(ctx, collectCtx, cancel)
	c.drain(ctx, cancel)
	c.stopWorkers()
	return c.finish(ctx)
}

func (c *collectionRun) startCollector(collectCtx context.Context, collect CollectSchedule) {
	c.collectDone = make(chan collectionResult, 1)

	c.updateCh = make(chan model.ScheduleUpdate, eitsCollectionBuffer)
	c.pfCh = make(chan model.PresentFollowing, eitsCollectionBuffer)
	go func() {
		onSchedule := func(update model.ScheduleUpdate) error {
			if !c.index.matchesCollectionNetwork(update.Service) {
				return nil
			}
			select {
			case c.updateCh <- update:
			case <-collectCtx.Done():
				return collectCtx.Err()
			}
			return nil
		}
		onPresentFollowing := func(pf model.PresentFollowing) error {
			if !c.index.matchesExpected(pf.Service) {
				return nil
			}
			select {
			case c.pfCh <- pf:
			case <-collectCtx.Done():
				return collectCtx.Err()
			}
			return nil
		}
		c.collectDone <- collectionResult{collectErr: collect(collectCtx, onSchedule, onPresentFollowing)}
	}()
}

func (c *collectionRun) startWorkers(collectCtx context.Context) {
	c.pfUpserts = newEITPFUpserter(collectCtx, c.events)
	c.partialFlushes = newPartialEITSFlusher(collectCtx, c.events)
	c.flushTicker = time.NewTicker(partialEITSFlushInterval)
	c.deadStreamTimer = time.NewTimer(eitsDeadStreamTimeout)
	c.stableTimer = time.NewTimer(time.Hour)
	c.stableTimer.Stop()
	c.idleTimer = time.NewTimer(time.Hour)
	c.idleTimer.Stop()
}

func (c *collectionRun) stopWorkers() {
	c.pfUpserts.stop()
	c.pfUpserts.wait()
	c.partialFlushes.stop()
	c.partialFlushes.wait()
}

func (c *collectionRun) handleUpdate(update model.ScheduleUpdate) {
	c.scheduleUpdates++
	key := update.Service
	if mapped, ok := c.schedule.unknownStream[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}]; ok {
		key = mapped
	}
	previous := c.schedule.updates[key]
	target := c.index.matchesExpected(update.Service)
	key = c.schedule.observe(update, target)
	c.telemetry.observe(update, previous, target)
	if target {
		c.idleTimer.Stop()
		c.idleC = nil
		if c.option.canYield(c.schedule, c.expected) {
			c.idleTimer.Reset(c.option.UnknownExtendedIdle)
			c.idleC = c.idleTimer.C
		}
		c.stableTimer.Stop()
		c.stableC = nil
		if targetTablesComplete(c.schedule, c.expected) {
			c.stableTimer.Reset(eitsStableStopDuration)
			c.stableC = c.stableTimer.C
		}
	}
	c.dirtyServices[key] = struct{}{}
	c.observedServices[key] = struct{}{}
}

func (c *collectionRun) handlePresentFollowing(pf model.PresentFollowing) {
	c.pfUpdates++
	c.telemetry.firstReceive("present_following")
	var current []model.Event
	for _, event := range []*model.Event{pf.Present, pf.Following} {
		if event != nil {
			current = append(current, *event)
		}
	}
	c.pfUpserts.enqueue(current)
}

func (c *collectionRun) receive(ctx, collectCtx context.Context, cancel context.CancelFunc) {
	finished := false
	for !finished {
		select {
		case update := <-c.updateCh:
			c.received++
			c.handleUpdate(update)
		case pf := <-c.pfCh:
			c.received++
			c.handlePresentFollowing(pf)
		case <-c.flushTicker.C:
			if c.partialFlushes.flush(c.schedule, c.dirtyServices) {
				c.dirtyServices = make(map[model.ServiceKey]struct{})
			}
		case <-c.stableC:
			c.stableC = nil
			for pending := len(c.updateCh); pending > 0; pending-- {
				c.handleUpdate(<-c.updateCh)
			}
			if c.schedule.stableFor(eitsStableStopDuration) && shouldStopEITSCollection(c.schedule, c.expected) {
				c.endReason = "complete"
				finished = true
			}
		case <-c.idleC:
			c.idleC = nil
			for pending := len(c.updateCh); pending > 0; pending-- {
				c.handleUpdate(<-c.updateCh)
			}
			if c.option.canYield(c.schedule, c.expected) && c.schedule.stableFor(c.option.UnknownExtendedIdle) {
				c.endReason = "unknown_extended_idle"
				finished = true
			}
		case <-c.deadStreamTimer.C:
			if c.received == 0 {
				slog.Warn("aborting EPG collection: no EIT sections received, tuner may be connected to a dead stream",
					"expectedServices", len(c.expected),
					"waited", eitsDeadStreamTimeout)
				c.endReason = "no_eit"
				finished = true
			}
		case c.collectorResult = <-c.collectDone:
			c.collectorDone = true
			c.endReason = "stream_ended"
			if c.collectorResult.collectErr != nil {
				c.endReason = "receive_error"
			}
			if collectCtx.Err() != nil {
				c.endReason = "deadline"
			}
			if ctx.Err() != nil {
				c.endReason = "canceled"
			}
			finished = true
			cancel()
		case <-collectCtx.Done():
			if ctx.Err() != nil {
				c.endReason = "canceled"
			}
			finished = true
		}
	}
}

// Drain before stopping workers: buffered PF updates still enqueue writes.
func (c *collectionRun) drain(ctx context.Context, cancel context.CancelFunc) {
	cancel()
	if !c.collectorDone {
		if ctx.Err() != nil {
			slog.Debug("skipping EPG collector drain during shutdown", "err", ctx.Err())
		} else {
			select {
			case c.collectorResult = <-c.collectDone:
				c.collectorDone = true
			case <-time.After(2 * time.Second):
			}
		}
	}
	// The collector may have buffered updates that the loop never got to
	// before collectDone or the deadline won the select. Drain them so
	// already-reported progress is not dropped.
	for drained := false; !drained; {
		select {
		case update := <-c.updateCh:
			c.handleUpdate(update)
		case pf := <-c.pfCh:
			c.handlePresentFollowing(pf)
		default:
			drained = true
		}
	}
}

func (c *collectionRun) finish(ctx context.Context) (*CollectResult, error) {
	var receiveErr error
	if c.collectorDone {
		expectedCancellation := (errors.Is(c.collectorResult.collectErr, context.Canceled) || errors.Is(c.collectorResult.collectErr, context.DeadlineExceeded)) && c.endReason != "receive_error"
		if c.collectorResult.collectErr != nil && !expectedCancellation {
			receiveErr = c.collectorResult.collectErr
			c.endReason = "receive_error"
			if c.scheduleUpdates == 0 && c.pfUpdates == 0 && errors.Is(receiveErr, tuner.ErrTunerUnavailable) {
				c.result.EndReason = "unavailable"
				return c.result, receiveErr
			}
			slog.Warn("EPG reception failed", "err", receiveErr)
		}
		if pfErr := c.pfUpserts.Err(); pfErr != nil {
			slog.Debug("EITPF upsert finished with error", "err", pfErr)
		}
	}
	slog.Debug("EPG collection updates observed",
		"eitpfUpdates", c.pfUpdates,
		"eitsUpdates", c.scheduleUpdates,
		"observedServices", len(c.observedServices),
		"expectedServices", len(c.expected))

	persistCtx := ctx
	if ctx.Err() != nil {
		var persistCancel context.CancelFunc
		persistCtx, persistCancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer persistCancel()
	}
	c.result.EndReason = c.endReason
	collectErr := c.persistObservedSnapshots(persistCtx, receiveErr)
	if ctx.Err() != nil {
		c.endReason = "canceled"
	}
	c.result.EndReason = c.endReason
	if receiveErr != nil {
		c.result.ReceiveError = receiveErr.Error()
	}
	c.telemetry.finish(c.schedule, c.expected, c.endReason, c.result, receiveErr)
	c.telemetry.span.SetAttributes(attribute.Int("epg.schedule_updates", c.scheduleUpdates), attribute.Int("epg.present_following_updates", c.pfUpdates))
	slog.Info("EPG collection finished", "reason", c.endReason, "targetServices", len(c.expected), "incompleteTargets", len(c.result.Incomplete), "programs", c.result.ProgramCount)
	return c.result, errors.Join(collectErr, ctx.Err())
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
