package program

import (
	"context"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/model"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return NewManager(NewSQLiteStore(database))
}

func TestListFiltersAndSorts(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 2), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 2, StartAt: testPtr[int64](2000), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 3, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 3}, EventID: 1, StartAt: testPtr[int64](500), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}

	serviceID := uint16(2)
	programs, err := manager.List(ctx, Query{ServiceID: &serviceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(programs) != 2 {
		t.Fatalf("len = %d, want 2", len(programs))
	}
	if programs[0].EventID != 1 || programs[1].EventID != 2 {
		t.Fatalf("programs not sorted by start time: %#v", programs)
	}
}

func TestListFiltersByID(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	wanted := ProgramID(1, 2, 1)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: wanted, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, FreeCA: true}},
		{ID: ProgramID(1, 2, 2), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 2, FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	programs, err := manager.List(ctx, Query{ID: &wanted})
	if err != nil {
		t.Fatal(err)
	}
	if len(programs) != 1 || programs[0].ID != wanted {
		t.Fatalf("programs = %#v, want ID %d", programs, wanted)
	}
}

func TestSQLiteStoreRejectsInvalidJSON(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	id := ProgramID(1, 2, 1)
	_, err = database.Write.ExecContext(ctx, `INSERT INTO programs
		(id, event_id, service_id, network_id, start_at, duration, is_free, event)
		VALUES (?, 1, 2, 1, 0, 0, 1, '{')`, id)
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLiteStore(database)
	if _, _, err := store.Get(ctx, id); err == nil {
		t.Fatal("Get succeeded with invalid event JSON")
	}
	if _, err := store.List(ctx, Query{}); err == nil {
		t.Fatal("List succeeded with invalid event JSON")
	}
}

func TestReplaceServiceProgramsDeletesFutureAndKeepsPast(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	now := int64(10000)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), FreeCA: true}},
		{ID: ProgramID(1, 2, 2), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 2, StartAt: testPtr[int64](5000), DurationMS: testPtr[int](1000), FreeCA: true}},
		{ID: ProgramID(1, 2, 3), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 3, StartAt: testPtr[int64](9000), DurationMS: testPtr[int](2000), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceServicePrograms(ctx, 1, 2, now, []*Program{
		{ID: ProgramID(1, 2, 4), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 4, StartAt: testPtr[int64](12000), DurationMS: testPtr[int](1000), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	serviceID := uint16(2)
	programs, err := manager.List(ctx, Query{ServiceID: &serviceID})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(programs), 3; got != want {
		t.Fatalf("after replace programs = %d, want %d", got, want)
	}
	if programs[0].EventID != 1 || programs[1].EventID != 2 {
		t.Fatalf("past programs not preserved: %#v", programs)
	}
	if programs[2].EventID != 4 {
		t.Fatalf("newest kept = %d, want 4", programs[2].EventID)
	}
}

func TestReplaceServiceProgramsReplacesAcrossServices(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](5000), DurationMS: testPtr[int](1000), FreeCA: true}},
		{ID: ProgramID(1, 3, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 3}, EventID: 1, StartAt: testPtr[int64](5000), DurationMS: testPtr[int](1000), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceServicePrograms(ctx, 1, 2, 0, nil); err != nil {
		t.Fatal(err)
	}
	other := uint16(3)
	got, err := manager.List(ctx, Query{ServiceID: &other})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("other service = %d, want 1", len(got))
	}
}

func TestSQLiteStoreRoundTripsExtendedAndRelatedAndSeries(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	id := ProgramID(1, 2, 1)
	nid, sid := uint16(1), uint16(2)
	program := &Program{ID: id, Event: model.Event{
		Key: model.ServiceKey{NetworkID: nid, StreamID: 0x4010, ServiceID: sid}, EventID: 1,
		StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "name", FreeCA: true,
		Extended: []model.ExtendedBlock{{Language: "jpn", Items: []model.ExtendedItem{{Name: "出演者", Text: "foo"}, {Name: "概要", Text: "bar"}}, Body: "body"}},
		Related:  []model.RelatedEvent{{GroupType: model.EventGroupShared, NetworkID: nid, ServiceID: sid, EventID: 9}},
		Series:   &model.Series{ID: 7, Repeat: 0, Pattern: testPtr(0), Episode: 1, LastEpisode: 12, Name: "series-name"},
		Parental: []model.ParentalRating{{Country: "jpn", Age: 13}},
	}}
	if err := manager.store.UpsertAll(ctx, []*Program{program}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := manager.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	// The whole event survives the store, stream ID and extended order
	// included.
	if !reflect.DeepEqual(got, program) {
		t.Fatalf("stored program = %#v, want %#v", got, program)
	}
}

func TestUpsertProgramsKeepsExistingDetailsWhenIncomingIsSparse(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	id := ProgramID(1, 2, 1)
	existing := &Program{ID: id, Event: model.Event{
		Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1,
		StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000),
		Name: "existing title", Description: "existing description",
		Genres:   []model.Genre{{Lv1: 0, Lv2: 1, Un1: 15, Un2: 15}},
		Videos:   []model.VideoComponent{{Codec: model.VideoCodecMPEG2, Resolution: model.VideoResolution1080i, Aspect: model.VideoAspect16x9NoPanVector}},
		Audios:   []model.AudioComponent{{ComponentType: 1, Languages: []string{"jpn"}}},
		Extended: []model.ExtendedBlock{{Items: []model.ExtendedItem{{Name: "出演者", Text: "existing cast"}}}},
		Series:   &model.Series{ID: 7, Name: "existing series"},
	}}
	if err := manager.UpsertPrograms(ctx, []*Program{existing}); err != nil {
		t.Fatal(err)
	}

	if err := manager.UpsertPrograms(ctx, []*Program{
		{ID: id, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](2000), DurationMS: testPtr[int](2000), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := manager.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if *got.StartAt != 2000 || *got.DurationMS != 2000 || !got.FreeCA {
		t.Fatalf("event fields = start:%d duration:%d freeCA:%v", *got.StartAt, *got.DurationMS, got.FreeCA)
	}
	if got.Name != existing.Name || got.Description != existing.Description {
		t.Fatalf("text fields = %q/%q", got.Name, got.Description)
	}
	if len(got.Genres) != 1 || len(got.Videos) != 1 || len(got.Audios) != 1 || len(got.Extended) != 1 || got.Series == nil {
		t.Fatalf("details were not preserved: %#v", got)
	}
}

// recordingProgramStore wraps a real Store and counts calls to the
// write methods, so a test can assert that an unchanged UpsertPrograms or
// ReplaceServicePrograms call skipped the underlying write entirely.
type recordingProgramStore struct {
	Store
	upsertCalls  int
	lastUpsert   []*Program
	replaceCalls int
}

func (s *recordingProgramStore) UpsertAll(ctx context.Context, programs []*Program) error {
	s.upsertCalls++
	s.lastUpsert = programs
	return s.Store.UpsertAll(ctx, programs)
}

func (s *recordingProgramStore) ReplaceServicePrograms(ctx context.Context, networkID, serviceID uint16, from int64, programs []*Program) error {
	s.replaceCalls++
	return s.Store.ReplaceServicePrograms(ctx, networkID, serviceID, from, programs)
}

func TestUpsertProgramsSkipsWriteWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	p := &Program{ID: ProgramID(1, 2, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "title", FreeCA: true}}
	seed := NewManager(NewSQLiteStore(database))
	if err := seed.UpsertPrograms(ctx, []*Program{p}); err != nil {
		t.Fatal(err)
	}

	recording := &recordingProgramStore{Store: NewSQLiteStore(database)}
	manager := NewManager(recording)
	if err := manager.UpsertPrograms(ctx, []*Program{
		{ID: p.ID, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "title", FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if recording.upsertCalls != 0 {
		t.Fatalf("UpsertAll called %d times, want 0", recording.upsertCalls)
	}
}

func TestUpsertProgramsWritesOnlyChangedPrograms(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	unchanged := &Program{ID: ProgramID(1, 2, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "unchanged", FreeCA: true}}
	changed := &Program{ID: ProgramID(1, 2, 2), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 2, StartAt: testPtr[int64](2000), DurationMS: testPtr[int](1000), Name: "old name", FreeCA: true}}
	seed := NewManager(NewSQLiteStore(database))
	if err := seed.UpsertPrograms(ctx, []*Program{unchanged, changed}); err != nil {
		t.Fatal(err)
	}

	recording := &recordingProgramStore{Store: NewSQLiteStore(database)}
	manager := NewManager(recording)
	if err := manager.UpsertPrograms(ctx, []*Program{
		{ID: unchanged.ID, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "unchanged", FreeCA: true}},
		{ID: changed.ID, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 2, StartAt: testPtr[int64](2000), DurationMS: testPtr[int](1000), Name: "new name", FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if recording.upsertCalls != 1 {
		t.Fatalf("UpsertAll called %d times, want 1", recording.upsertCalls)
	}
	if len(recording.lastUpsert) != 1 || recording.lastUpsert[0].ID != changed.ID {
		t.Fatalf("UpsertAll programs = %#v, want only %d", recording.lastUpsert, changed.ID)
	}
}

func TestReplaceServiceProgramsSkipsWriteWhenIdentical(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()

	p := &Program{ID: ProgramID(1, 2, 1), Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "title", FreeCA: true}}
	seed := NewManager(NewSQLiteStore(database))
	if err := seed.ReplaceServicePrograms(ctx, 1, 2, 0, []*Program{p}); err != nil {
		t.Fatal(err)
	}

	recording := &recordingProgramStore{Store: NewSQLiteStore(database)}
	manager := NewManager(recording)
	if err := manager.ReplaceServicePrograms(ctx, 1, 2, 0, []*Program{
		{ID: p.ID, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "title", FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if recording.replaceCalls != 0 {
		t.Fatalf("ReplaceServicePrograms called %d times, want 0", recording.replaceCalls)
	}
}

func TestUpsertProgramsFillsSparseProgramWithLaterDetails(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	id := ProgramID(1, 2, 1)
	if err := manager.UpsertPrograms(ctx, []*Program{
		{ID: id, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := manager.UpsertPrograms(ctx, []*Program{
		{ID: id, Event: model.Event{Key: model.ServiceKey{NetworkID: 1, ServiceID: 2}, EventID: 1, StartAt: testPtr[int64](1000), DurationMS: testPtr[int](1000), Name: "later title", Description: "later description", Genres: []model.Genre{{Lv1: 2, Lv2: 3, Un1: 15, Un2: 15}}, Audios: []model.AudioComponent{{ComponentType: 3, Languages: []string{}}}, FreeCA: true}},
	}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := manager.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if got.Name != "later title" || got.Description != "later description" || len(got.Genres) != 1 || len(got.Audios) != 1 {
		t.Fatalf("program was not filled by later details: %#v", got)
	}
}
