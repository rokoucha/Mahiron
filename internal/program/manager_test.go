package program

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/db"
)

func newTestManager(t *testing.T) *ProgramManager {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return NewProgramManager(NewSQLiteStore(database))
}

func TestListFiltersAndSorts(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 2), NetworkID: 1, ServiceID: 2, EventID: 2, StartAt: 2000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), NetworkID: 1, ServiceID: 2, EventID: 1, StartAt: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 3, 1), NetworkID: 1, ServiceID: 3, EventID: 1, StartAt: 500},
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
		{ID: wanted, NetworkID: 1, ServiceID: 2, EventID: 1},
		{ID: ProgramID(1, 2, 2), NetworkID: 1, ServiceID: 2, EventID: 2},
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
	_, err = database.ExecContext(ctx, `INSERT INTO programs
		(id, event_id, service_id, network_id, start_at, duration, is_free, genres)
		VALUES (?, 1, 2, 1, 0, 0, 1, '{')`, id)
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLiteStore(database)
	if _, _, err := store.Get(ctx, id); err == nil {
		t.Fatal("Get succeeded with invalid genres JSON")
	}
	if _, err := store.List(ctx, Query{}); err == nil {
		t.Fatal("List succeeded with invalid genres JSON")
	}
}

func TestReplaceServiceProgramsDeletesFutureAndKeepsPast(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	now := int64(10000)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), NetworkID: 1, ServiceID: 2, EventID: 1, StartAt: 1000, Duration: 1000},
		{ID: ProgramID(1, 2, 2), NetworkID: 1, ServiceID: 2, EventID: 2, StartAt: 5000, Duration: 1000},
		{ID: ProgramID(1, 2, 3), NetworkID: 1, ServiceID: 2, EventID: 3, StartAt: 9000, Duration: 2000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceServicePrograms(ctx, 1, 2, now, []*Program{
		{ID: ProgramID(1, 2, 4), NetworkID: 1, ServiceID: 2, EventID: 4, StartAt: 12000, Duration: 1000},
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
		{ID: ProgramID(1, 2, 1), NetworkID: 1, ServiceID: 2, EventID: 1, StartAt: 5000, Duration: 1000},
		{ID: ProgramID(1, 3, 1), NetworkID: 1, ServiceID: 3, EventID: 1, StartAt: 5000, Duration: 1000},
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
	program := &Program{
		ID:        id,
		NetworkID: nid,
		ServiceID: sid,
		EventID:   1,
		StartAt:   1000,
		Duration:  1000,
		Name:      "name",
		Extended:  map[string]string{"出演者": "foo", "概要": "bar"},
		RelatedItems: []RelatedItem{
			{Type: RelatedItemTypeShared, NetworkID: &nid, ServiceID: sid, EventID: 9},
		},
		Series: &Series{ID: 7, Repeat: 0, Pattern: 0, Episode: 1, LastEpisode: 12, Name: "series-name"},
	}
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
	if got.Extended["出演者"] != "foo" {
		t.Fatalf("Extended[出演者] = %q", got.Extended["出演者"])
	}
	if len(got.RelatedItems) != 1 || got.RelatedItems[0].Type != RelatedItemTypeShared {
		t.Fatalf("RelatedItems = %#v", got.RelatedItems)
	}
	if got.Series == nil || got.Series.ID != 7 || got.Series.Name != "series-name" {
		t.Fatalf("Series = %#v", got.Series)
	}
}

func TestUpsertProgramsKeepsExistingDetailsWhenIncomingIsSparse(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	id := ProgramID(1, 2, 1)
	existing := &Program{
		ID:          id,
		NetworkID:   1,
		ServiceID:   2,
		EventID:     1,
		StartAt:     1000,
		Duration:    1000,
		IsFree:      true,
		Name:        "existing title",
		Description: "existing description",
		Genres:      []Genre{{Lv1: 0, Lv2: 1, Un1: 15, Un2: 15}},
		Video:       &Video{StreamContent: 1, ComponentType: 179},
		Audios:      []Audio{{ComponentType: 1}},
		Extended:    map[string]string{"出演者": "existing cast"},
		Series:      &Series{ID: 7, Name: "existing series"},
	}
	if err := manager.UpsertPrograms(ctx, []*Program{existing}); err != nil {
		t.Fatal(err)
	}

	if err := manager.UpsertPrograms(ctx, []*Program{{
		ID:        id,
		NetworkID: 1,
		ServiceID: 2,
		EventID:   1,
		StartAt:   2000,
		Duration:  2000,
		IsFree:    false,
	}}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := manager.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if got.StartAt != 2000 || got.Duration != 2000 || got.IsFree {
		t.Fatalf("event fields = start:%d duration:%d isFree:%v", got.StartAt, got.Duration, got.IsFree)
	}
	if got.Name != existing.Name || got.Description != existing.Description {
		t.Fatalf("text fields = %q/%q", got.Name, got.Description)
	}
	if len(got.Genres) != 1 || got.Video == nil || len(got.Audios) != 1 || got.Extended["出演者"] != "existing cast" || got.Series == nil {
		t.Fatalf("details were not preserved: %#v", got)
	}
}

func TestUpsertProgramsFillsSparseProgramWithLaterDetails(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	id := ProgramID(1, 2, 1)
	if err := manager.UpsertPrograms(ctx, []*Program{{
		ID:        id,
		NetworkID: 1,
		ServiceID: 2,
		EventID:   1,
		StartAt:   1000,
		Duration:  1000,
	}}); err != nil {
		t.Fatal(err)
	}

	if err := manager.UpsertPrograms(ctx, []*Program{{
		ID:          id,
		NetworkID:   1,
		ServiceID:   2,
		EventID:     1,
		StartAt:     1000,
		Duration:    1000,
		Name:        "later title",
		Description: "later description",
		Genres:      []Genre{{Lv1: 2, Lv2: 3, Un1: 15, Un2: 15}},
		Audios:      []Audio{{ComponentType: 3}},
	}}); err != nil {
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
