package epggather

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

func TestKnownServiceProgramUpdaterFiltersUnknownServicesAfterRefresh(t *testing.T) {
	inner := &recordingProgramUpdater{}
	lister := &recordingServiceLister{
		services: []*service.Service{{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 101}}}},
	}
	updater := NewKnownServiceProgramUpdater(inner, lister)

	err := updater.UpsertEvents(context.Background(), []model.Event{
		{Key: model.ServiceKey{NetworkID: 4, ServiceID: 101}, EventID: 1},
		{Key: model.ServiceKey{NetworkID: 4, ServiceID: 102}, EventID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(inner.programs), 1; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if inner.programs[0].Key.ServiceID != 101 {
		t.Fatalf("program = %#v, want known service", inner.programs[0])
	}
	if got, want := lister.calls, 2; got != want {
		t.Fatalf("service list calls = %d, want %d (initial load and unknown refresh)", got, want)
	}
}

func TestKnownServiceProgramUpdaterRefreshesUnknownOnce(t *testing.T) {
	inner := &recordingProgramUpdater{}
	lister := &recordingServiceLister{
		services: []*service.Service{{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 101}}}},
		refreshServices: []*service.Service{
			{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 101}}},
			{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 102}}},
		},
	}
	updater := NewKnownServiceProgramUpdater(inner, lister)

	err := updater.UpsertEvents(context.Background(), []model.Event{
		{Key: model.ServiceKey{NetworkID: 4, ServiceID: 102}, EventID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(inner.programs), 1; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if inner.programs[0].Key.ServiceID != 102 {
		t.Fatalf("program = %#v, want refreshed service", inner.programs[0])
	}
	if got, want := lister.calls, 2; got != want {
		t.Fatalf("service list calls = %d, want %d (initial load and unknown refresh)", got, want)
	}
}

type recordingProgramUpdater struct {
	removed  []int64
	programs []model.Event
}

func (u *recordingProgramUpdater) UpsertEvents(_ context.Context, programs []model.Event) error {
	u.programs = append(u.programs, programs...)
	return nil
}

type recordingServiceLister struct {
	calls           int
	services        []*service.Service
	refreshServices []*service.Service
}

func (l *recordingServiceLister) GetServices(context.Context) ([]*service.Service, error) {
	l.calls++
	if l.calls > 1 && l.refreshServices != nil {
		return l.refreshServices, nil
	}
	return l.services, nil
}

func (u *recordingProgramUpdater) RemovePrograms(_ context.Context, ids []int64) error {
	u.removed = append(u.removed, ids...)
	return nil
}

func TestRemoteRemovalsFilterUnknownServices(t *testing.T) {
	inner := &recordingProgramUpdater{}
	loader := &recordingServiceLister{services: []*service.Service{{Service: model.Service{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}}}}}
	updater := NewKnownServiceProgramUpdater(inner, loader)
	known := model.ProgramID(model.ServiceKey{NetworkID: 1, ServiceID: 2}, 1)
	unknown := model.ProgramID(model.ServiceKey{NetworkID: 2, ServiceID: 2}, 1)
	if err := updater.RemovePrograms(t.Context(), []int64{known, unknown, -1, 999999999999999999}); err != nil {
		t.Fatal(err)
	}
	if len(inner.removed) != 1 || inner.removed[0] != known {
		t.Fatalf("removed=%v", inner.removed)
	}
}
