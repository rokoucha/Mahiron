package program

import "context"

// RemovePrograms applies authoritative remote removals and only publishes
// notifications after the database transaction succeeds.
func (m *Manager) RemovePrograms(ctx context.Context, ids []int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, err := m.store.ListByIDs(ctx, ids)
	if err != nil {
		return err
	}
	removed := make([]int64, 0, len(existing))
	for _, p := range existing {
		removed = append(removed, p.ID)
	}
	if len(removed) == 0 {
		return nil
	}
	if err := m.store.UpsertAndDelete(ctx, nil, removed); err != nil {
		return err
	}
	for _, id := range removed {
		delete(m.eit, id)
		delete(m.eitExtended, id)
		m.enqueueProgramRemoveEvent(id)
	}
	return nil
}
