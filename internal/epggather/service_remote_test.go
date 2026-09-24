package epggather

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/ts"
)

func TestCollectServiceSnapshotsSyncsStoredRemotePrograms(t *testing.T) {
	ctx := context.Background()
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	store := newRemoteSyncProgramStore()
	status := newRemoteSyncServiceStore()
	session := &remoteSyncSession{
		programs: map[ServiceKey][]*program.Program{
			key: {{ID: program.ProgramID(4, 101, 1), NetworkID: 4, ServiceID: 101, EventID: 1}},
		},
	}

	if _, err := CollectServiceSnapshots(ctx, store, status, session, []ServiceKey{key}, time.Second); err != nil {
		t.Fatal(err)
	}
	if session.collectEITCalled {
		t.Fatal("remote stored-program sync should not call EIT collectors")
	}
	if len(store.replaced[key]) != 1 || store.replaced[key][0].EventID != 1 {
		t.Fatalf("replaced = %#v", store.replaced)
	}
	if store.sources[key] != "remote" {
		t.Fatalf("replace source = %q, want remote", store.sources[key])
	}
	if status.attempts[key] == 0 {
		t.Fatal("attempt timestamp was not recorded")
	}
	if status.successes[key] == 0 {
		t.Fatal("success timestamp was not recorded")
	}
	if status.errors[key] != "" {
		t.Fatalf("last error = %q, want empty", status.errors[key])
	}
}

func TestCollectServiceSnapshotsSyncsStoredRemoteProgramsPartialFailure(t *testing.T) {
	ctx := context.Background()
	okKey := ServiceKey{NetworkID: 4, ServiceID: 101}
	failKey := ServiceKey{NetworkID: 4, ServiceID: 102}
	wantErr := errors.New("remote unavailable")
	store := newRemoteSyncProgramStore()
	status := newRemoteSyncServiceStore()
	session := &remoteSyncSession{
		programs: map[ServiceKey][]*program.Program{
			okKey: {{ID: program.ProgramID(4, 101, 1), NetworkID: 4, ServiceID: 101, EventID: 1}},
		},
		errs: map[ServiceKey]error{failKey: wantErr},
	}

	_, err := CollectServiceSnapshots(ctx, store, status, session, []ServiceKey{okKey, failKey}, time.Second)
	if err == nil {
		t.Fatal("CollectServiceSnapshots error = nil, want partial failure")
	}
	if len(store.replaced[okKey]) != 1 {
		t.Fatalf("successful service was not replaced: %#v", store.replaced)
	}
	if _, ok := store.replaced[failKey]; ok {
		t.Fatalf("failed service was replaced: %#v", store.replaced)
	}
	if status.successes[okKey] == 0 {
		t.Fatal("successful service did not record success")
	}
	if status.successes[failKey] != 0 {
		t.Fatal("failed service recorded success")
	}
	if status.errors[failKey] != wantErr.Error() {
		t.Fatalf("failed service error = %q, want %q", status.errors[failKey], wantErr.Error())
	}
}

type remoteSyncProgramStore struct {
	replaced map[ServiceKey][]*program.Program
	sources  map[ServiceKey]string
}

func newRemoteSyncProgramStore() *remoteSyncProgramStore {
	return &remoteSyncProgramStore{
		replaced: make(map[ServiceKey][]*program.Program),
		sources:  make(map[ServiceKey]string),
	}
}

func (s *remoteSyncProgramStore) UpsertPrograms(context.Context, []*program.Program) error {
	return errors.New("UpsertPrograms should not be called")
}

func (s *remoteSyncProgramStore) DeleteEndedBefore(context.Context, int64) error {
	return nil
}

func (s *remoteSyncProgramStore) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, _ int64, programs []*program.Program) error {
	key := ServiceKey{NetworkID: networkID, ServiceID: serviceID}
	s.replaced[key] = append([]*program.Program(nil), programs...)
	s.sources[key] = observability.EPGMetricSource(ctx)
	return nil
}

type remoteSyncServiceStore struct {
	attempts  map[ServiceKey]int64
	successes map[ServiceKey]int64
	errors    map[ServiceKey]string
}

func newRemoteSyncServiceStore() *remoteSyncServiceStore {
	return &remoteSyncServiceStore{
		attempts:  make(map[ServiceKey]int64),
		successes: make(map[ServiceKey]int64),
		errors:    make(map[ServiceKey]string),
	}
}

func (s *remoteSyncServiceStore) GetServices(context.Context) ([]*service.Service, error) {
	return nil, nil
}

func (s *remoteSyncServiceStore) SetEPGAttempt(_ context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error {
	key := ServiceKey{NetworkID: networkID, ServiceID: serviceID}
	s.attempts[key] = attemptedAt
	s.errors[key] = lastError
	return nil
}

func (s *remoteSyncServiceStore) SetEPGSuccess(_ context.Context, networkID, serviceID uint16, succeededAt int64) error {
	key := ServiceKey{NetworkID: networkID, ServiceID: serviceID}
	s.attempts[key] = succeededAt
	s.successes[key] = succeededAt
	s.errors[key] = ""
	return nil
}

type remoteSyncSession struct {
	programs         map[ServiceKey][]*program.Program
	errs             map[ServiceKey]error
	collectEITCalled bool
}

func (s *remoteSyncSession) ListServicePrograms(_ context.Context, networkID, serviceID uint16) ([]*program.Program, error) {
	key := ServiceKey{NetworkID: networkID, ServiceID: serviceID}
	if err := s.errs[key]; err != nil {
		return nil, err
	}
	return s.programs[key], nil
}

func (s *remoteSyncSession) CollectEIT(context.Context, func(*ts.EIT) error) error {
	s.collectEITCalled = true
	return nil
}
