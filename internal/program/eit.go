package program

import (
	"context"
	"github.com/21S1298001/mahiron/internal/model"
)

type eitState struct {
	revision model.EITRevision
	endAt    int64
}

type eitTable struct {
	service model.ServiceKey
	source  string
	tableID uint8
}

func tableOf(service model.ServiceKey, revision model.EITRevision) eitTable {
	source := revision.Source
	if !revision.Other() {
		source = ""
	}
	return eitTable{service: service, source: source, tableID: revision.TableID}
}

func (m *Manager) acceptEIT(p *Program, tables map[eitTable]model.EITRevision, existing *Program) bool {
	if p.EITRevision == nil {
		return true
	}
	if latest, ok := tables[tableOf(p.Key, *p.EITRevision)]; ok && !p.EITRevision.PreferTo(latest) {
		return false
	}
	if previous, ok := m.eit[p.ID]; ok {
		return p.EITRevision.PreferTo(previous.revision)
	}
	if existing != nil {
		if existing.EITRevision != nil {
			return p.EITRevision.PreferTo(*existing.EITRevision)
		}
		if p.EITRevision.Other() {
			return false
		}
	}
	return true
}

func (m *Manager) acceptExtended(p *Program, tables map[eitTable]model.EITRevision, existing *Program) bool {
	revision := p.EITExtendedRevision
	if latest, ok := tables[tableOf(p.Key, *revision)]; ok && !revision.PreferTo(latest) {
		return false
	}
	if previous, ok := m.eitExtended[p.ID]; ok {
		return revision.PreferTo(previous.revision)
	}
	if existing != nil {
		if existing.EITExtendedRevision != nil {
			return revision.PreferTo(*existing.EITExtendedRevision)
		}
		if len(existing.Extended) > 0 && revision.Other() {
			return false
		}
	}
	return true
}

// Provenance only orders in-flight writes. It is process-local: a restart
// discards both the buffered snapshots and these reception generations.
func (m *Manager) recordEIT(programs map[int64]*Program) {
	if m.eit == nil {
		m.eit = make(map[int64]eitState)
	}
	if m.eitExtended == nil {
		m.eitExtended = make(map[int64]eitState)
	}
	if m.eitTables == nil {
		m.eitTables = make(map[eitTable]model.EITRevision)
	}
	for id, p := range programs {
		for kind, revision := range []*model.EITRevision{p.EITRevision, p.EITExtendedRevision} {
			if revision == nil {
				continue
			}
			states := m.eit
			if kind == 1 {
				states = m.eitExtended
			}
			previous, ok := states[id]
			if ok && previous.revision.PreferTo(*revision) && previous.revision.Reception > revision.Reception {
				continue
			}
			states[id] = eitState{revision: *revision, endAt: p.StartAtOrZero() + int64(p.DurationOrZero())}
			key := tableOf(p.Key, *revision)
			if latest, ok := m.eitTables[key]; !ok || revision.Reception > latest.Reception {
				m.eitTables[key] = *revision
			}
		}
	}
}

func (m *Manager) UpsertSchedule(ctx context.Context, snapshot model.ScheduleSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eitTables == nil {
		m.eitTables = make(map[eitTable]model.EITRevision)
	}
	for _, revision := range snapshot.Revisions {
		source := revision.Source
		if !revision.Other() {
			source = ""
		}
		key := eitTable{service: snapshot.Service, source: source, tableID: revision.TableID}
		if previous, ok := m.eitTables[key]; !ok || revision.Reception > previous.Reception {
			m.eitTables[key] = revision
		}
	}
	programs := make([]*Program, len(snapshot.Events))
	for i := range snapshot.Events {
		programs[i] = FromEvent(snapshot.Events[i])
	}
	return m.upsertPrograms(ctx, programs)
}

func preferPending(previous, incoming *Program) *Program {
	if previous == nil {
		return incoming
	}
	preferred := *incoming
	if previous.EITRevision != nil && incoming.EITRevision != nil && !incoming.EITRevision.PreferTo(*previous.EITRevision) {
		preferred = *previous
	}
	extended := incoming
	if previous.EITExtendedRevision != nil && (incoming.EITExtendedRevision == nil || !incoming.EITExtendedRevision.PreferTo(*previous.EITExtendedRevision)) {
		extended = previous
	}
	if extended.EITExtendedRevision != nil {
		preferred.Extended = extended.Extended
		preferred.EITExtendedRevision = extended.EITExtendedRevision
	}
	return &preferred
}
