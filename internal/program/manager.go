package program

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
)

const programEventDelay = time.Second

const (
	eventTypeCreate = "create"
	eventTypeUpdate = "update"
	eventTypeRemove = "remove"
)

type eventPublisher interface {
	PublishProgramEvent(typ string, data map[string]any)
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
		if ok && reflect.DeepEqual(existing, after) {
			continue
		}
		toWrite = append(toWrite, p)
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

func (m *ProgramManager) Get(ctx context.Context, id int64) (*Program, bool, error) {
	return m.store.Get(ctx, id)
}

func (m *ProgramManager) List(ctx context.Context, query Query) ([]*Program, error) {
	return m.store.List(ctx, query)
}

func (m *ProgramManager) DeleteEndedBefore(ctx context.Context, cutoff int64) error {
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

func (m *ProgramManager) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
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
		case !reflect.DeepEqual(existing, p):
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

func (m *ProgramManager) Count(ctx context.Context) (int, error) { return m.store.Count(ctx) }

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
		if !ok || !reflect.DeepEqual(existing, p) {
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

func mergeUpsertProgram(existing, incoming *Program) *Program {
	if incoming == nil {
		return nil
	}
	merged := cloneProgram(incoming)
	if existing == nil {
		return merged
	}
	if merged.Name == "" {
		merged.Name = existing.Name
	}
	if merged.Description == "" {
		merged.Description = existing.Description
	}
	if len(merged.Genres) == 0 {
		merged.Genres = cloneGenres(existing.Genres)
	}
	if merged.Video == nil {
		merged.Video = cloneVideo(existing.Video)
	}
	if len(merged.Audios) == 0 {
		merged.Audios = cloneAudios(existing.Audios)
	}
	if len(merged.Extended) == 0 {
		merged.Extended = cloneStringMap(existing.Extended)
	}
	if len(merged.RelatedItems) == 0 {
		merged.RelatedItems = cloneRelatedItems(existing.RelatedItems)
	}
	if merged.Series == nil {
		merged.Series = cloneSeries(existing.Series)
	}
	return merged
}

func cloneProgram(p *Program) *Program {
	if p == nil {
		return nil
	}
	clone := *p
	clone.Genres = cloneGenres(p.Genres)
	clone.Video = cloneVideo(p.Video)
	clone.Audios = cloneAudios(p.Audios)
	clone.Extended = cloneStringMap(p.Extended)
	clone.RelatedItems = cloneRelatedItems(p.RelatedItems)
	clone.Series = cloneSeries(p.Series)
	return &clone
}

func cloneGenres(items []Genre) []Genre {
	return append([]Genre(nil), items...)
}

func cloneVideo(video *Video) *Video {
	if video == nil {
		return nil
	}
	clone := *video
	return &clone
}

func cloneAudios(items []Audio) []Audio {
	if len(items) == 0 {
		return nil
	}
	clones := make([]Audio, len(items))
	for i := range items {
		clones[i] = items[i]
		clones[i].ComponentTag = cloneInt(items[i].ComponentTag)
		clones[i].IsMain = cloneBool(items[i].IsMain)
		clones[i].SamplingRate = cloneInt(items[i].SamplingRate)
		clones[i].Langs = append([]string(nil), items[i].Langs...)
	}
	return clones
}

func cloneInt(v *int) *int {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}

func cloneBool(v *bool) *bool {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}

func cloneStringMap(items map[string]string) map[string]string {
	if len(items) == 0 {
		return nil
	}
	clone := make(map[string]string, len(items))
	for k, v := range items {
		clone[k] = v
	}
	return clone
}

func cloneRelatedItems(items []RelatedItem) []RelatedItem {
	if len(items) == 0 {
		return nil
	}
	clones := make([]RelatedItem, len(items))
	for i := range items {
		clones[i] = items[i]
		clones[i].NetworkID = cloneUint16(items[i].NetworkID)
		clones[i].TransportStreamID = cloneUint16(items[i].TransportStreamID)
	}
	return clones
}

func cloneUint16(v *uint16) *uint16 {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}

func cloneSeries(series *Series) *Series {
	if series == nil {
		return nil
	}
	clone := *series
	if series.ExpiresAt != nil {
		expiresAt := *series.ExpiresAt
		clone.ExpiresAt = &expiresAt
	}
	return &clone
}

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
			m.events.PublishProgramEvent(event.typ, map[string]any{"id": event.removeID})
		} else {
			m.events.PublishProgramEvent(event.typ, event.program.EventData())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
