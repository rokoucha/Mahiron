package program

import (
	"context"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
)

const programEventDelay = time.Second

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
	eventTypeRemove = "remove"
)

type eventPublisher interface {
	PublishProgramEvent(typ string, event *model.Event)
	PublishProgramRemove(typ string, id int64)
}

type Manager struct {
	store      Store
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

func NewManager(store Store, events ...eventPublisher) *Manager {
	m := &Manager{store: store}
	if len(events) > 0 {
		m.events = events[0]
	}
	return m
}

// UpsertEvents stores broadcast events, merging each with the stored program
// of the same ID.
func (m *Manager) UpsertEvents(ctx context.Context, events []model.Event) error {
	if len(events) == 0 {
		return nil
	}
	programs := make([]*Program, len(events))
	for i := range events {
		programs[i] = FromEvent(events[i])
	}
	return m.UpsertPrograms(ctx, programs)
}

func (m *Manager) UpsertPrograms(ctx context.Context, programs []*Program) error {
	source := observability.EPGMetricSource(ctx)
	attempted := nonNilProgramCount(programs)
	pending := make(map[int64]*Program, len(programs))
	ids := make([]int64, 0, len(programs))
	for _, p := range programs {
		if p == nil {
			continue
		}
		if _, ok := pending[p.ID]; !ok {
			ids = append(ids, p.ID)
		}
		pending[p.ID] = p
	}
	existingPrograms, err := m.store.ListByIDs(ctx, ids)
	if err != nil {
		return err
	}
	before := make(map[int64]*Program, len(existingPrograms))
	for _, p := range existingPrograms {
		before[p.ID] = p
	}

	type pendingEvent struct {
		typ   string
		after *Program
	}
	toWrite := make([]*Program, 0, len(ids))
	events := make([]pendingEvent, 0, len(ids))
	for _, id := range ids {
		p := pending[id]
		existing, ok := before[id]
		after := mergeUpsertProgram(existing, p)
		if ok && sameProgram(existing, after) {
			continue
		}
		toWrite = append(toWrite, after)
		if !ok {
			events = append(events, pendingEvent{typ: eventTypeCreate, after: after})
		} else {
			events = append(events, pendingEvent{typ: eventTypeUpdate, after: after})
		}
	}

	if len(toWrite) == 0 {
		observability.RecordEPGProgramsUpserted(ctx, source, "success", 0)
		return nil
	}

	if err := m.store.UpsertAll(ctx, toWrite); err != nil {
		observability.RecordEPGProgramsUpserted(ctx, source, "error", int64(attempted))
		return err
	}
	for _, ev := range events {
		m.enqueueProgramEvent(ev.typ, ev.after)
	}
	observability.RecordEPGProgramsUpserted(ctx, source, "success", int64(len(events)))
	return nil
}

func (m *Manager) Get(ctx context.Context, id int64) (*Program, bool, error) {
	return m.store.Get(ctx, id)
}

func (m *Manager) List(ctx context.Context, query Query) ([]*Program, error) {
	return m.store.List(ctx, query)
}

func (m *Manager) ListFunc(ctx context.Context, query Query, yield func(*Program) error) error {
	return m.store.ListFunc(ctx, query, yield)
}

func (m *Manager) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
	source := observability.EPGMetricSource(ctx)
	removed, err := m.store.ListEndedIDsBefore(ctx, cutoff)
	if err != nil {
		return err
	}
	if err := m.store.DeleteEndedBefore(ctx, cutoff); err != nil {
		observability.RecordEPGProgramsDeleted(ctx, source, "error", int64(len(removed)))
		return err
	}
	for _, id := range removed {
		m.enqueueProgramRemoveEvent(id)
	}
	observability.RecordEPGProgramsDeleted(ctx, source, "success", int64(len(removed)))
	return nil
}

// DeleteExpired deletes the programs that ended more than retentionDays
// before now. A retentionDays of 0 or less keeps every program.
func (m *Manager) DeleteExpired(ctx context.Context, now time.Time, retentionDays int) error {
	if retentionDays <= 0 {
		return nil
	}
	cutoff := now.Add(-time.Duration(retentionDays) * 24 * time.Hour).UnixMilli()
	return m.DeleteEndedBefore(observability.ContextWithEPGMetricSource(ctx, "cleanup"), cutoff)
}

func (m *Manager) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	source := observability.EPGMetricSource(ctx)
	attempted := nonNilProgramCount(programs)
	beforeList, err := m.store.ListByServiceFrom(ctx, networkID, serviceID, from)
	if err != nil {
		return err
	}
	before := map[int64]*Program{}
	for _, p := range beforeList {
		before[p.ID] = p
	}

	if identicalServicePrograms(before, programs) {
		observability.RecordEPGProgramsUpserted(ctx, source, "success", 0)
		observability.RecordEPGProgramsDeleted(ctx, source, "success", 0)
		return nil
	}

	if err := m.store.ReplaceServicePrograms(ctx, networkID, serviceID, from, programs); err != nil {
		observability.RecordEPGProgramsUpserted(ctx, source, "error", int64(attempted))
		observability.RecordEPGProgramsDeleted(ctx, source, "error", int64(len(beforeList)))
		return err
	}
	changed := 0
	for _, p := range programs {
		if p == nil {
			continue
		}
		existing, ok := before[p.ID]
		delete(before, p.ID)
		switch {
		case !ok:
			changed++
			m.enqueueProgramEvent(eventTypeCreate, p)
		case !sameProgram(existing, p):
			changed++
			m.enqueueProgramEvent(eventTypeUpdate, p)
		}
	}
	for id := range before {
		m.enqueueProgramRemoveEvent(id)
	}
	observability.RecordEPGProgramsUpserted(ctx, source, "success", int64(changed))
	observability.RecordEPGProgramsDeleted(ctx, source, "success", int64(len(before)))
	return nil
}

func (m *Manager) Count(ctx context.Context) (int, error) { return m.store.Count(ctx) }

// identicalServicePrograms reports whether incoming exactly matches the
// existing rows keyed by ID, with no additions or removals.
func identicalServicePrograms(before map[int64]*Program, incoming []*Program) bool {
	count := 0
	for _, p := range incoming {
		if p == nil {
			continue
		}
		count++
		existing, ok := before[p.ID]
		if !ok || !sameProgram(existing, p) {
			return false
		}
	}
	return count == len(before)
}

func nonNilProgramCount(programs []*Program) int {
	count := 0
	for _, p := range programs {
		if p != nil {
			count++
		}
	}
	return count
}

// mergeUpsertProgram fills what incoming lacks from the stored program: a
// basic EIT table carries names and components, an extended table only the
// extended description, and p/f and schedule updates arrive separately.
func mergeUpsertProgram(existing, incoming *Program) *Program {
	if incoming == nil {
		return nil
	}
	merged := *incoming
	if existing == nil {
		return &merged
	}
	if merged.Name == "" {
		merged.Name = existing.Name
	}
	if merged.Description == "" {
		merged.Description = existing.Description
	}
	if merged.Language == "" {
		merged.Language = existing.Language
	}
	if len(merged.Genres) == 0 {
		merged.Genres = existing.Genres
	}
	if len(merged.Videos) == 0 {
		merged.Videos = existing.Videos
	}
	if len(merged.Audios) == 0 {
		merged.Audios = existing.Audios
	}
	if len(merged.Extended) == 0 {
		merged.Extended = existing.Extended
	}
	if len(merged.Related) == 0 {
		merged.Related = existing.Related
	}
	if len(merged.Parental) == 0 {
		merged.Parental = existing.Parental
	}
	if merged.Series == nil {
		merged.Series = existing.Series
	}
	return &merged
}

func (m *Manager) enqueueProgramEvent(typ string, p *Program) {
	if m.events == nil {
		return
	}
	m.enqueueEvent(programEvent{typ: typ, program: p})
}

func (m *Manager) enqueueProgramRemoveEvent(id int64) {
	if m.events == nil {
		return
	}
	m.enqueueEvent(programEvent{typ: eventTypeRemove, removeID: id})
}

func (m *Manager) enqueueEvent(event programEvent) {
	m.eventMu.Lock()
	defer m.eventMu.Unlock()
	m.eventQueue = append(m.eventQueue, event)
	if m.eventTimer != nil {
		m.eventTimer.Reset(programEventDelay)
		return
	}
	m.eventTimer = time.AfterFunc(programEventDelay, m.flushEvents)
}

func (m *Manager) flushEvents() {
	m.eventMu.Lock()
	queue := append([]programEvent(nil), m.eventQueue...)
	m.eventQueue = nil
	m.eventTimer = nil
	m.eventMu.Unlock()

	for _, event := range queue {
		if event.typ == eventTypeRemove {
			m.events.PublishProgramRemove(event.typ, event.removeID)
		} else {
			m.events.PublishProgramEvent(event.typ, &event.program.Event)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
