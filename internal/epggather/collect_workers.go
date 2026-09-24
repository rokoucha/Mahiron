package epggather

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
)

var ErrEITPFQueueOverflow = errors.New("eitpf upsert queue overflow")

type partialEITSFlusher struct {
	ctx       context.Context
	events    EventWriter
	requests  chan []model.Event
	done      chan struct{}
	closeOnce sync.Once
}

func newPartialEITSFlusher(ctx context.Context, events EventWriter) *partialEITSFlusher {
	f := &partialEITSFlusher{
		ctx:      observability.ContextWithEPGMetricSource(ctx, "eits"),
		events:   events,
		requests: make(chan []model.Event, 1),
		done:     make(chan struct{}),
	}
	go f.run()
	return f
}

func (f *partialEITSFlusher) flush(schedule *collectionSchedule, dirty map[model.ServiceKey]struct{}) bool {
	if schedule == nil || len(dirty) == 0 {
		return true
	}
	var programs []model.Event
	for key := range dirty {
		programs = append(programs, schedule.events(key)...)
	}
	if len(programs) == 0 {
		return true
	}
	select {
	case f.requests <- programs:
		return true
	default:
		slog.Debug("skipping partial EITS flush while previous flush is still running", "programs", len(programs))
		return false
	}
}

func (f *partialEITSFlusher) stop() {
	f.closeOnce.Do(func() {
		close(f.requests)
	})
}

func (f *partialEITSFlusher) wait() {
	<-f.done
}

func (f *partialEITSFlusher) run() {
	defer close(f.done)
	for programs := range f.requests {
		if err := f.events.UpsertEvents(f.ctx, programs); err != nil {
			slog.Debug("partial EITS upsert finished with error", "err", err)
		}
	}
}

type eitPFUpserter struct {
	ctx       context.Context
	events    EventWriter
	requests  chan []model.Event
	done      chan struct{}
	closeOnce sync.Once

	mu      sync.Mutex
	err     error
	pending int
}

func newEITPFUpserter(ctx context.Context, events EventWriter) *eitPFUpserter {
	u := &eitPFUpserter{
		ctx:      observability.ContextWithEPGMetricSource(ctx, "eitpf"),
		events:   events,
		requests: make(chan []model.Event, eitsCollectionBuffer),
		done:     make(chan struct{}),
	}
	go u.run()
	return u
}

func (u *eitPFUpserter) enqueue(programs []model.Event) {
	if len(programs) == 0 {
		return
	}
	u.mu.Lock()
	if u.err != nil || u.pending != 0 {
		u.mu.Unlock()
		return
	}
	u.pending++
	u.mu.Unlock()
	select {
	case u.requests <- programs:
	default:
		u.mu.Lock()
		u.pending--
		u.mu.Unlock()
		u.setErr(ErrEITPFQueueOverflow)
	}
}

func (u *eitPFUpserter) stop() {
	u.closeOnce.Do(func() {
		close(u.requests)
	})
}

func (u *eitPFUpserter) wait() {
	<-u.done
}

func (u *eitPFUpserter) Err() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.err
}

func (u *eitPFUpserter) setErr(err error) {
	if err == nil {
		return
	}
	u.mu.Lock()
	if u.err == nil {
		u.err = err
	}
	u.mu.Unlock()
}

func (u *eitPFUpserter) run() {
	defer close(u.done)
	for programs := range u.requests {
		if err := u.events.UpsertEvents(u.ctx, programs); err != nil {
			u.setErr(err)
			slog.Debug("EITPF upsert finished with error", "err", err)
		}
		u.mu.Lock()
		u.pending--
		u.mu.Unlock()
	}
}
