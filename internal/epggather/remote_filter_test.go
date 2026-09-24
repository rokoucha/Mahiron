package epggather

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
)

func TestKnownServiceProgramUpdaterFiltersUnknownServicesAfterRefresh(t *testing.T) {
	inner := &recordingProgramUpdater{}
	lister := &recordingServiceLister{
		services: []*service.Service{{NetworkId: 4, ServiceId: 101}},
	}
	updater := NewKnownServiceProgramUpdater(inner, lister)

	err := updater.UpsertPrograms(context.Background(), []*program.Program{
		{ID: 401010001, NetworkID: 4, ServiceID: 101, EventID: 1},
		{ID: 401020001, NetworkID: 4, ServiceID: 102, EventID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(inner.programs), 1; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if inner.programs[0].ServiceID != 101 {
		t.Fatalf("program = %#v, want known service", inner.programs[0])
	}
	if got, want := lister.calls, 2; got != want {
		t.Fatalf("service list calls = %d, want %d (initial load and unknown refresh)", got, want)
	}
}

func TestKnownServiceProgramUpdaterRefreshesUnknownOnce(t *testing.T) {
	inner := &recordingProgramUpdater{}
	lister := &recordingServiceLister{
		services: []*service.Service{{NetworkId: 4, ServiceId: 101}},
		refreshServices: []*service.Service{
			{NetworkId: 4, ServiceId: 101},
			{NetworkId: 4, ServiceId: 102},
		},
	}
	updater := NewKnownServiceProgramUpdater(inner, lister)

	err := updater.UpsertPrograms(context.Background(), []*program.Program{
		{ID: 401020001, NetworkID: 4, ServiceID: 102, EventID: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(inner.programs), 1; got != want {
		t.Fatalf("upserted programs = %d, want %d", got, want)
	}
	if inner.programs[0].ServiceID != 102 {
		t.Fatalf("program = %#v, want refreshed service", inner.programs[0])
	}
	if got, want := lister.calls, 2; got != want {
		t.Fatalf("service list calls = %d, want %d (initial load and unknown refresh)", got, want)
	}
}

type recordingProgramUpdater struct {
	programs []*program.Program
}

func (u *recordingProgramUpdater) UpsertPrograms(_ context.Context, programs []*program.Program) error {
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
