package channel

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

// dataBroadcastQueueSize bounds completed DSM-CC section work without
// blocking the TS demux loop. DII is handled synchronously so the carousel is
// always registered before its DDB blocks enter this queue.
const dataBroadcastQueueSize = 1024
const dataBroadcastPriorityQueueSize = 256
const dataBroadcastPriorityBurst = 8

// dataBroadcastSnapshotFlushInterval bounds how stale a persisted provisional
// snapshot can be. It is a periodic sweep rather than an event-driven queue:
// PMT/DII sections rarely change once a service is stable, so most ticks scan
// unchanged state and write nothing.
const dataBroadcastSnapshotFlushInterval = 30 * time.Second

// bmlWorker owns the BML (TS data broadcast) background work of a channel
// session: the DDB queue pump feeding the Hub and the periodic persistence
// of provisional snapshots. The session itself keeps only stream plumbing
// (demuxers, EIT/logo updates) and delegates BMLSource to this worker.
type bmlWorker struct {
	hub           *bml.Hub
	queue         chan ts.PIDSection
	priorityQueue chan ts.PIDSection
	wg            sync.WaitGroup
	done          chan struct{}
	snapshotDone  chan struct{}
	snapshotStore bml.SnapshotStore
	lastPersisted map[uint16]bml.PersistedService
	typ           string
	channel       string
}

func newBMLWorker(typ, channelID string, hub *bml.Hub, snapshotStore bml.SnapshotStore) *bmlWorker {
	return &bmlWorker{
		hub:           hub,
		queue:         make(chan ts.PIDSection, dataBroadcastQueueSize),
		priorityQueue: make(chan ts.PIDSection, dataBroadcastPriorityQueueSize),
		snapshotStore: snapshotStore,
		lastPersisted: map[uint16]bml.PersistedService{},
		typ:           typ,
		channel:       channelID,
	}
}

func (w *bmlWorker) observePacket(packet ts.Packet) {
	if w.hub == nil {
		return
	}
	w.hub.ObservePacket(packet)
}

func (w *bmlWorker) observePIDSection(section ts.PIDSection) {
	if w.hub == nil {
		return
	}
	if section.Section.TableID() != ts.TableIDDSMCCDDB {
		w.hub.Observe(section)
		return
	}
	priority, entryDocument := w.hub.DDBPriority(section)
	queue := w.queue
	operation := "ddb_queue"
	if entryDocument || priority > 0 {
		queue = w.priorityQueue
		operation = "ddb_priority_queue"
	}
	w.wg.Add(1)
	select {
	case queue <- section:
	default:
		w.wg.Done()
		observability.RecordDataBroadcastCarouselEvent(context.Background(), w.typ, w.channel, operation, "overflow")
		slog.Warn("data broadcast DDB queue overflow", "type", w.typ, "channel", w.channel, "priority", priority, "entryDocument", entryDocument)
	}
}

func (w *bmlWorker) start(ctx context.Context) {
	if w.done != nil {
		return
	}
	w.done = make(chan struct{})
	w.snapshotDone = make(chan struct{})
	go w.runUpdates(ctx, w.done)
	go w.runSnapshotPersist(ctx, w.snapshotDone)
}

func (w *bmlWorker) stop() {
	if w.done != nil {
		<-w.done
	}
	if w.snapshotDone != nil {
		<-w.snapshotDone
	}
}

func (w *bmlWorker) runUpdates(ctx context.Context, done chan struct{}) {
	defer close(done)
	priorityBurst := 0
	for {
		section, ok := w.nextSection(ctx, &priorityBurst)
		if !ok {
			// Sections were already accepted from the demuxer. Finish the bounded
			// backlog so a final module completion is not lost on input shutdown.
			for {
				select {
				case section := <-w.priorityQueue:
					w.observeQueuedDDB(section)
				case section := <-w.queue:
					w.observeQueuedDDB(section)
				default:
					return
				}
			}
		}
		w.observeQueuedDDB(section)
	}
}

// nextSection favors entry/high-cache-priority modules, but
// forces one normal section after a bounded burst. Emergency broadcasts can
// keep the priority queue continuously non-empty; without this fairness bound,
// ordinary modules remain announced forever and their HTTP URLs return 425.
func (w *bmlWorker) nextSection(ctx context.Context, priorityBurst *int) (ts.PIDSection, bool) {
	if *priorityBurst >= dataBroadcastPriorityBurst {
		select {
		case section := <-w.queue:
			*priorityBurst = 0
			return section, true
		default:
		}
	}
	select {
	case section := <-w.priorityQueue:
		*priorityBurst++
		return section, true
	default:
	}
	select {
	case <-ctx.Done():
		return ts.PIDSection{}, false
	case section := <-w.priorityQueue:
		*priorityBurst++
		return section, true
	case section := <-w.queue:
		*priorityBurst = 0
		return section, true
	}
}

func (w *bmlWorker) observeQueuedDDB(section ts.PIDSection) {
	w.hub.Observe(section)
	w.wg.Done()
}

// runSnapshotPersist periodically writes the current PMT/DII
// state to the snapshot store so a future GetOrCreate for this channel can
// serve a provisional /state response before a tuner is reacquired. It runs
// even when no store is configured so its done channel is always closed,
// keeping worker start/stop symmetric with the other two update workers.
func (w *bmlWorker) runSnapshotPersist(ctx context.Context, done chan struct{}) {
	defer close(done)
	if w.snapshotStore == nil {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(dataBroadcastSnapshotFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.flushSnapshots()
			return
		case <-ticker.C:
			w.flushSnapshots()
		}
	}
}

// flushSnapshots writes only services whose persisted state
// changed since the last flush. It is called from a single goroutine
// (runSnapshotPersist), so lastPersisted needs no lock.
func (w *bmlWorker) flushSnapshots() {
	if w.hub == nil || w.snapshotStore == nil {
		return
	}
	for _, persisted := range w.hub.PersistableState() {
		if previous, ok := w.lastPersisted[persisted.ServiceID]; ok && persistedServiceEqual(previous, persisted) {
			continue
		}
		if err := w.snapshotStore.PutSnapshot(w.typ, w.channel, persisted); err != nil {
			slog.Warn("failed to persist data broadcast snapshot", "type", w.typ, "channel", w.channel, "serviceId", persisted.ServiceID, "err", err)
			continue
		}
		w.lastPersisted[persisted.ServiceID] = persisted
	}
}

func persistedServiceEqual(a, b bml.PersistedService) bool {
	if a.ServiceID != b.ServiceID || !bytes.Equal(a.PMTSection, b.PMTSection) || len(a.Carousels) != len(b.Carousels) {
		return false
	}
	for i := range a.Carousels {
		if a.Carousels[i].ComponentTag != b.Carousels[i].ComponentTag ||
			a.Carousels[i].PID != b.Carousels[i].PID ||
			!bytes.Equal(a.Carousels[i].DIISection, b.Carousels[i].DIISection) {
			return false
		}
	}
	return true
}
