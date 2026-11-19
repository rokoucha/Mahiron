package channel

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

const (
	dataBroadcastQueueSize = 1280
	// Keep space for signaling when a burst of DDB blocks fills the queue.
	dataBroadcastSignalingReserve = 256
	dataBroadcastDrainTimeout     = 2 * time.Second
)

type bmlUpdate struct {
	section ts.PIDSection
	pcr     bml.PCR
	pid     uint16
	barrier chan struct{}
}

// dataBroadcastSnapshotFlushInterval bounds how stale a persisted provisional
// snapshot can be. It is a periodic sweep rather than an event-driven queue:
// PMT/DII sections rarely change once a service is stable, so most ticks scan
// unchanged state and write nothing.
const dataBroadcastSnapshotFlushInterval = 30 * time.Second

// bmlWorker owns the BML (TS data broadcast) background work of a channel
// session: the signaling and carousel queue pump feeding the Hub and the periodic persistence
// of provisional snapshots. The session itself keeps only stream plumbing
// (demuxers, EIT/logo updates) and delegates BMLSource to this worker.
type bmlWorker struct {
	hub           *bml.Hub
	queue         chan bmlUpdate
	lastPMT       map[uint16]ts.Section
	dropped       atomic.Int64
	done          chan struct{}
	snapshotDone  chan struct{}
	cancelWork    context.CancelFunc
	snapshotStore bml.SnapshotStore
	lastPersisted map[uint16]bml.PersistedService
	typ           string
	channel       string
}

func newBMLWorker(typ, channelID string, hub *bml.Hub, snapshotStore bml.SnapshotStore) *bmlWorker {
	return &bmlWorker{
		hub:           hub,
		queue:         make(chan bmlUpdate, dataBroadcastQueueSize),
		lastPMT:       make(map[uint16]ts.Section),
		snapshotStore: snapshotStore,
		lastPersisted: map[uint16]bml.PersistedService{},
		typ:           typ,
		channel:       channelID,
	}
}

func (w *bmlWorker) observePacket(packet ts.Packet) {
	if base, extension, ok := packet.ProgramClockReference(); ok {
		w.enqueue(bmlUpdate{pid: packet.PID(), pcr: bml.PCR{PCRBase: base, PCRExtension: extension}}, true)
	}
}

func (w *bmlWorker) observePIDSection(section ts.PIDSection) {
	// SectionAssembler gives each completed section its own backing storage.
	update := bmlUpdate{section: section}
	switch section.Section.TableID() {
	case ts.TableIDPMT:
		// A repeated PMT would be ignored by the Hub after parsing. Only
		// remember accepted PMTs so an overflowed change can be retried.
		if len(section.Section) < 5 {
			return
		}
		serviceID := binary.BigEndian.Uint16(section.Section[3:5])
		if !bytes.Equal(w.lastPMT[serviceID], section.Section) && w.enqueue(update, false) {
			w.lastPMT[serviceID] = section.Section
		}
	case ts.TableIDDSMCCDDB:
		w.enqueue(update, true)
	case ts.TableIDDSMCCDII, ts.TableIDDSMCCStream,
		ts.TableIDBIT, ts.TableIDEITPF0, ts.TableIDEITPF1, ts.TableIDTOT:
		w.enqueue(update, false)
	}
}

func (w *bmlWorker) enqueue(update bmlUpdate, bulk bool) bool {
	if !bulk || len(w.queue) < cap(w.queue)-dataBroadcastSignalingReserve {
		select {
		case w.queue <- update:
			return true
		default:
		}
	}
	w.dropped.Add(1)
	return false
}

func (w *bmlWorker) start(ctx context.Context) {
	if w.done != nil {
		return
	}
	w.done = make(chan struct{})
	w.snapshotDone = make(chan struct{})
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	w.cancelWork = cancelWork
	go w.runUpdates(ctx, workCtx)
	go w.runSnapshotPersist(ctx, workCtx)
}

func (w *bmlWorker) stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, dataBroadcastDrainTimeout)
	defer cancel()
	defer w.cancelWork()
	select {
	case <-w.snapshotDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForUpdates waits for events already accepted by the demuxer at EOF.
func (w *bmlWorker) waitForUpdates(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, dataBroadcastDrainTimeout)
	defer cancel()
	barrier := make(chan struct{})
	select {
	case w.queue <- bmlUpdate{barrier: barrier}:
	case <-w.done:
		return
	case <-ctx.Done():
		return
	}
	select {
	case <-barrier:
	case <-w.done:
	case <-ctx.Done():
	}
}

func (w *bmlWorker) runUpdates(stopCtx, workCtx context.Context) {
	defer close(w.done)
	for workCtx.Err() == nil {
		select {
		case <-workCtx.Done():
			return
		case <-stopCtx.Done():
			// The demuxer stopped invoking hooks. Drain accepted work while
			// the caller's shutdown budget permits.
			for workCtx.Err() == nil {
				select {
				case update := <-w.queue:
					w.observeUpdate(workCtx, update)
				default:
					return
				}
			}
			return
		case update := <-w.queue:
			w.observeUpdate(workCtx, update)
		}
	}
}

func (w *bmlWorker) observeUpdate(ctx context.Context, update bmlUpdate) {
	switch {
	case update.barrier != nil:
		close(update.barrier)
	case update.section.Section != nil:
		w.hub.ObserveContext(ctx, update.section)
	default:
		w.hub.ObservePCR(update.pid, update.pcr)
	}
}

func (w *bmlWorker) runSnapshotPersist(stopCtx, workCtx context.Context) {
	defer func() {
		<-w.done
		close(w.snapshotDone)
	}()
	ticker := time.NewTicker(dataBroadcastSnapshotFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-workCtx.Done():
			return
		case <-stopCtx.Done():
			select {
			case <-w.done:
				w.reportDrops()
				w.flushSnapshots(workCtx)
			case <-workCtx.Done():
			}
			return
		case <-ticker.C:
			w.reportDrops()
			w.flushSnapshots(workCtx)
		}
	}
}

func (w *bmlWorker) reportDrops() {
	if count := w.dropped.Swap(0); count > 0 {
		observability.RecordDataBroadcastCarouselEvent(context.Background(), w.typ, w.channel, "update_queue", "overflow")
		slog.Warn("data broadcast update queue overflow", "type", w.typ, "channel", w.channel, "dropped", count)
	}
}

// flushSnapshots writes only services whose persisted state
// changed since the last flush. It is called from a single goroutine
// (runSnapshotPersist), so lastPersisted needs no lock.
func (w *bmlWorker) flushSnapshots(ctx context.Context) {
	if w.hub == nil || w.snapshotStore == nil {
		return
	}
	store, contextual := w.snapshotStore.(bml.ContextSnapshotStore)
	for _, persisted := range w.hub.PersistableState() {
		if ctx.Err() != nil {
			return
		}
		if previous, ok := w.lastPersisted[persisted.ServiceID]; ok && persistedServiceEqual(previous, persisted) {
			continue
		}
		var err error
		if contextual {
			err = store.PutSnapshotContext(ctx, w.typ, w.channel, persisted)
		} else {
			err = w.snapshotStore.PutSnapshot(w.typ, w.channel, persisted)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
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
