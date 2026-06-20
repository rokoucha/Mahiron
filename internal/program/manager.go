package program

import (
	"context"
	"reflect"
	"sync"
	"time"
)

const programEventDelay = time.Second

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
	eventTypeRemove = "remove"
)

type eventPublisher interface {
	PublishProgramEvent(typ string, p *Program)
	PublishProgramRemoveEvent(id int64)
}

type ProgramManager struct {
	store      ProgramStore
	events     eventPublisher
	eventMu    sync.Mutex
	eventTimer *time.Timer
	eventQueue []programEvent
}

type programEvent struct {
	typ      string
	program  *Program
	removeID int64
}

func NewProgramManager(store ProgramStore, events ...eventPublisher) *ProgramManager {
	m := &ProgramManager{store: store}
	if len(events) > 0 {
		m.events = events[0]
	}
	return m
}

func (m *ProgramManager) UpsertPrograms(ctx context.Context, programs []*Program) error {
	before := make(map[int64]*Program, len(programs))
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok, err := m.store.Get(ctx, p.ID)
		if err != nil {
			return err
		}
		if ok {
			before[p.ID] = existing
		}
	}
	if err := m.store.UpsertAll(ctx, programs); err != nil {
		return err
	}
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok := before[p.ID]
		switch {
		case !ok:
			m.enqueueProgramEvent(eventTypeCreate, p)
		case !reflect.DeepEqual(existing, p):
			m.enqueueProgramEvent(eventTypeUpdate, p)
		}
	}
	return nil
}

func (m *ProgramManager) Get(ctx context.Context, id int64) (*Program, bool, error) {
	return m.store.Get(ctx, id)
}

func (m *ProgramManager) List(ctx context.Context, query Query) ([]*Program, error) {
	return m.store.List(ctx, query)
}

func (m *ProgramManager) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
	programs, err := m.store.List(ctx, Query{})
	if err != nil {
		return err
	}
	removed := make([]int64, 0)
	for _, p := range programs {
		if p.StartAt+int64(p.Duration) < cutoff {
			removed = append(removed, p.ID)
		}
	}
	if err := m.store.DeleteEndedBefore(ctx, cutoff); err != nil {
		return err
	}
	for _, id := range removed {
		m.enqueueProgramRemoveEvent(id)
	}
	return nil
}

func (m *ProgramManager) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	beforeList, err := m.store.List(ctx, Query{NetworkID: &networkID, ServiceID: &serviceID})
	if err != nil {
		return err
	}
	before := map[int64]*Program{}
	for _, p := range beforeList {
		if p.StartAt >= from {
			before[p.ID] = p
		}
	}
	if err := m.store.ReplaceServicePrograms(ctx, networkID, serviceID, from, programs); err != nil {
		return err
	}
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok := before[p.ID]
		delete(before, p.ID)
		switch {
		case !ok:
			m.enqueueProgramEvent(eventTypeCreate, p)
		case !reflect.DeepEqual(existing, p):
			m.enqueueProgramEvent(eventTypeUpdate, p)
		}
	}
	for id := range before {
		m.enqueueProgramRemoveEvent(id)
	}
	return nil
}

func (m *ProgramManager) Count(ctx context.Context) (int, error) { return m.store.Count(ctx) }

func (m *ProgramManager) enqueueProgramEvent(typ string, p *Program) {
	if m.events == nil {
		return
	}
	m.enqueueEvent(programEvent{typ: typ, program: p})
}

func (m *ProgramManager) enqueueProgramRemoveEvent(id int64) {
	if m.events == nil {
		return
	}
	m.enqueueEvent(programEvent{typ: eventTypeRemove, removeID: id})
}

func (m *ProgramManager) enqueueEvent(event programEvent) {
	m.eventMu.Lock()
	defer m.eventMu.Unlock()
	m.eventQueue = append(m.eventQueue, event)
	if m.eventTimer != nil {
		m.eventTimer.Reset(programEventDelay)
		return
	}
	m.eventTimer = time.AfterFunc(programEventDelay, m.flushEvents)
}

func (m *ProgramManager) flushEvents() {
	m.eventMu.Lock()
	queue := append([]programEvent(nil), m.eventQueue...)
	m.eventQueue = nil
	m.eventTimer = nil
	m.eventMu.Unlock()

	for _, event := range queue {
		if event.typ == eventTypeRemove {
			m.events.PublishProgramRemoveEvent(event.removeID)
		} else {
			m.events.PublishProgramEvent(event.typ, event.program)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
