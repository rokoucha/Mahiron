package program

import (
	"context"

	"github.com/21S1298001/mahiron/internal/model"
)

func (m *Manager) UpsertPresentFollowing(ctx context.Context, snapshot model.PresentFollowingSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	programs := make([]*Program, 0, len(snapshot.Events))
	for _, event := range snapshot.Events {
		programs = append(programs, FromEvent(event))
	}
	return m.upsertPrograms(ctx, programs, &snapshot)
}

func (m *Manager) reconcilePresentFollowing(ctx context.Context, programs []*Program, snapshot *model.PresentFollowingSnapshot) ([]*Program, map[int64]bool, error) {
	stored, err := m.store.List(ctx, Query{NetworkID: &snapshot.Service.NetworkID, ServiceID: &snapshot.Service.ServiceID})
	if err != nil {
		return nil, nil, err
	}
	current := make(map[int64]bool, len(programs))
	for _, p := range programs {
		current[p.ID] = true
	}
	retired := make(map[int64]bool)
	for _, p := range stored {
		if current[p.ID] || (!p.EITPresent && !p.EITFollowing) {
			continue
		}
		// Keep known history, but do not invent an end time for an undefined event
		// that disappeared from both slots (TR-B14 19.3, C.5.7).
		if p.DurationOrZero() <= 0 && (p.EITPresent || p.EITSuspended) {
			retired[p.ID] = true
			continue
		}
		p.EITPresent = false
		p.EITFollowing = false
		p.EITSuspended = false
		programs = append(programs, p)
	}
	return programs, retired, nil
}
