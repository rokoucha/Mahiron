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
	mu          sync.Mutex
	eit         map[int64]eitState
	eitExtended map[int64]eitState
	eitTables   map[eitTable]model.EITRevision
	store       Store
	events      eventPublisher
	eventMu     sync.Mutex
	eventTimer  *time.Timer
	eventQueue  []programEvent
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.upsertPrograms(ctx, programs, nil)
}

func (m *Manager) upsertPrograms(ctx context.Context, programs []*Program, pf *model.PresentFollowingSnapshot) error {
	retired := make(map[int64]bool)
	if pf != nil {
		var err error
		programs, retired, err = m.reconcilePresentFollowing(ctx, programs, pf)
		if err != nil {
			return err
		}
	}
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
		pending[p.ID] = preferPending(pending[p.ID], p)
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
	tableRevisions := make(map[eitTable]model.EITRevision, len(pending))
	for _, p := range pending {
		for _, revision := range []*model.EITRevision{p.EITRevision, p.EITExtendedRevision} {
			if revision == nil {
				continue
			}
			key := tableOf(p.Key, *revision)
			previous, ok := tableRevisions[key]
			if !ok {
				previous, ok = m.eitTables[key]
			}
			if !ok || revision.Reception > previous.Reception {
				previous = *revision
			}
			tableRevisions[key] = previous
		}
	}
	accepted := make(map[int64]*Program)
	toWrite := make([]*Program, 0, len(ids))
	events := make([]pendingEvent, 0, len(ids))
	for _, id := range ids {
		p := pending[id]
		existing, ok := before[id]
		var after *Program
		if m.acceptEIT(p, tableRevisions, existing) {
			after = mergeUpsertProgram(existing, p)
			if pf == nil && p.EITRevision != nil && existing != nil && (existing.EITPresent || existing.EITFollowing) {
				after = mergeUpsertProgram(p, existing)
			}
			accepted[id] = after
		} else {
			if !ok {
				continue
			}
			// A stale/other snapshot may fill missing details, but must not move or
			// rename an authoritative event or delete its overlapping neighbours.
			after = mergeUpsertProgram(p, existing)
			after.EITRevision = existing.EITRevision
			after.EITExtendedRevision = existing.EITExtendedRevision
		}
		if p.EITExtendedRevision != nil {
			if m.acceptExtended(p, tableRevisions, existing) {
				after.Extended = p.Extended
				after.EITExtendedRevision = p.EITExtendedRevision
				accepted[id] = after
			} else if existing != nil {
				after.Extended = existing.Extended
				after.EITExtendedRevision = existing.EITExtendedRevision
			} else {
				after.Extended = nil
				after.EITExtendedRevision = nil
			}
		}

		if pf != nil {
			after.EITPresent = p.EITPresent
			after.EITFollowing = p.EITFollowing
			after.EITSuspended = p.EITFollowing && existing != nil && (existing.EITPresent || existing.EITSuspended)
		}
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

	if len(toWrite) == 0 && len(retired) == 0 {
		m.recordEIT(accepted)
		observability.RecordEPGProgramsUpserted(ctx, source, "success", 0)
		return nil
	}

	removed := retired
	blockedWrites := map[int64]bool{}
	writtenByService := make(map[[2]uint16]map[int64]*Program)
	for _, p := range toWrite {
		service := [2]uint16{p.Key.NetworkID, p.Key.ServiceID}
		written := writtenByService[service]
		if written == nil {
			written = make(map[int64]*Program)
			writtenByService[service] = written
		}
		written[p.ID] = p
		delete(removed, p.ID)
		old := before[p.ID]
		if old != nil && old.StartAtOrZero() == p.StartAtOrZero() && old.DurationOrZero() == p.DurationOrZero() {
			continue
		}
		if p.StartAt == nil || p.DurationOrZero() <= 0 {
			continue
		}
		start, end := *p.StartAt, *p.StartAt+int64(p.DurationOrZero())
		candidates, err := m.store.List(ctx, Query{NetworkID: &p.Key.NetworkID, ServiceID: &p.Key.ServiceID, StartAt: &start, EndAt: &end})
		if err != nil {
			return err
		}
		latest := make(map[int64]*Program, len(candidates)+len(written))
		for _, other := range candidates {
			latest[other.ID] = other
		}
		for _, other := range written {
			latest[other.ID] = other
		}
		var overlaps []int64
		blocked := false
		for _, other := range latest {
			if other.ID == p.ID || other.Key.NetworkID != p.Key.NetworkID || other.Key.ServiceID != p.Key.ServiceID || other.StartAt == nil || other.DurationOrZero() <= 0 {
				continue
			}
			if pf != nil && (other.EITPresent || other.EITFollowing) {
				if slot := pending[other.ID]; slot != nil && (slot.EITPresent || slot.EITFollowing) {
					continue
				}
			}
			if (other.EITPresent || other.EITFollowing) && !pending[p.ID].EITPresent {
				continue
			}
			if start < *other.StartAt+int64(other.DurationOrZero()) && *other.StartAt < end {
				if p.EITRevision != nil {
					previous := other.EITRevision
					if previous == nil {
						if state, ok := m.eit[other.ID]; ok {
							previous = &state.revision
						}
					}
					if previous == nil && p.EITRevision.Other() || previous != nil && !p.EITRevision.PreferTo(*previous) {
						blocked = true
						break
					}
				}
				overlaps = append(overlaps, other.ID)
			}
		}
		if blocked {
			blockedWrites[p.ID] = true
			if old != nil {
				written[p.ID] = old
			} else {
				delete(written, p.ID)
			}
		} else {
			for _, id := range overlaps {
				removed[id] = true
			}
		}
	}

	kept := toWrite[:0]
	for _, p := range toWrite {
		if !removed[p.ID] && !blockedWrites[p.ID] {
			kept = append(kept, p)
		}
	}
	removedIDs := make([]int64, 0, len(removed))
	for id := range removed {
		removedIDs = append(removedIDs, id)
	}
	if len(removedIDs) == 0 {
		err = m.store.UpsertAll(ctx, kept)
	} else {
		err = m.store.UpsertAndDelete(ctx, kept, removedIDs)
	}
	if err != nil {
		observability.RecordEPGProgramsUpserted(ctx, source, "error", int64(attempted))
		return err
	}
	for id := range removed {
		delete(accepted, id)
	}
	for id := range blockedWrites {
		delete(accepted, id)
	}
	m.recordEIT(accepted)
	for _, ev := range events {
		if !removed[ev.after.ID] && !blockedWrites[ev.after.ID] {
			m.enqueueProgramEvent(ev.typ, ev.after)
		}
	}
	for _, id := range removedIDs {
		m.enqueueProgramRemoveEvent(id)
	}
	observability.RecordEPGProgramsDeleted(ctx, source, "success", int64(len(removedIDs)))
	observability.RecordEPGProgramsUpserted(ctx, source, "success", int64(len(kept)))
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
	m.mu.Lock()
	defer m.mu.Unlock()
	source := observability.EPGMetricSource(ctx)
	removed, err := m.store.ListEndedIDsBefore(ctx, cutoff)
	if err != nil {
		return err
	}
	if err := m.store.DeleteEndedBefore(ctx, cutoff); err != nil {
		observability.RecordEPGProgramsDeleted(ctx, source, "error", int64(len(removed)))
		return err
	}
	for _, states := range []map[int64]eitState{m.eit, m.eitExtended} {
		for id, state := range states {
			if state.endAt > 0 && state.endAt < cutoff {
				delete(states, id)
			}
		}
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
	m.mu.Lock()
	defer m.mu.Unlock()
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
	for _, p := range beforeList {
		delete(m.eit, p.ID)
		delete(m.eitExtended, p.ID)
	}
	for _, p := range programs {
		if p != nil {
			delete(m.eit, p.ID)
			delete(m.eitExtended, p.ID)
		}
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
	if merged.EITExtendedRevision == nil {
		merged.EITExtendedRevision = existing.EITExtendedRevision
	}
	if merged.EITRevision == nil {
		merged.EITRevision = existing.EITRevision
	}
	if !merged.EITPresent && !merged.EITFollowing {
		merged.EITPresent = existing.EITPresent
		merged.EITFollowing = existing.EITFollowing
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
