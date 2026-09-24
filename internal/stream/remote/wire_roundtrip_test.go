package remote

import (
	"context"
	"encoding/json"
	"testing"

	mahirondb "github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// countingStore counts the writes that reach the program store.
type countingStore struct {
	program.Store
	upserts int
}

func (s *countingStore) UpsertAll(ctx context.Context, programs []*program.Program) error {
	s.upserts++
	return s.Store.UpsertAll(ctx, programs)
}

// TestRemoteProgramRoundTripsThroughStore guards that program.Manager skips
// writing a remote program that hasn't changed: a program converted from a
// Mirakurun-style remote event must compare equal to the same program read
// back from the database, in particular for empty collections, which the
// wire JSON may omit or spell out as empty arrays and objects.
func TestRemoteProgramRoundTripsThroughStore(t *testing.T) {
	ctx := context.Background()
	database, err := mahirondb.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := &countingStore{Store: program.NewSQLiteStore(database)}
	manager := program.NewManager(store)

	cases := []struct {
		name string
		json string
	}{
		{
			name: "sparse program with omitted collections",
			json: `{"id":1,"eventId":1,"serviceId":1,"networkId":1,"startAt":1000,"duration":1000,"isFree":true,"name":"n","description":"d"}`,
		},
		{
			name: "program with empty collections spelled out in JSON",
			json: `{"id":2,"eventId":2,"serviceId":1,"networkId":1,"startAt":2000,"duration":1000,"isFree":true,"name":"n2","genres":[],"audios":[],"extended":{},"relatedItems":[]}`,
		},
		{
			name: "program with populated fields",
			json: `{"id":3,"eventId":3,"serviceId":1,"networkId":1,"startAt":3000,"duration":1000,"isFree":false,"name":"n3","description":"d3",
				"genres":[{"lv1":0,"lv2":1,"un1":15,"un2":15}],
				"video":{"streamContent":1,"componentType":179},
				"audios":[{"componentType":1,"componentTag":16,"isMain":true,"samplingRate":7,"langs":["jpn"]}],
				"extended":{"出演者":"foo"},
				"relatedItems":[{"type":"shared","networkId":1,"serviceId":1,"eventId":9}],
				"series":{"id":7,"repeat":0,"pattern":1,"expiresAt":123,"episode":1,"lastEpisode":12,"name":"series"}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := store.upserts
			if err := manager.UpsertEvents(ctx, []model.Event{decodeTestProgram(t, tc.json)}); err != nil {
				t.Fatal(err)
			}
			if err := manager.UpsertEvents(ctx, []model.Event{decodeTestProgram(t, tc.json)}); err != nil {
				t.Fatal(err)
			}
			if got := store.upserts - before; got != 1 {
				t.Fatalf("store writes = %d, want 1: the unchanged second upsert must be skipped", got)
			}
		})
	}
}

func decodeTestProgram(t *testing.T, raw string) model.Event {
	t.Helper()
	var api apigen.Program
	if err := json.Unmarshal([]byte(raw), &api); err != nil {
		t.Fatal(err)
	}
	return mirakurun.EventFromAPI(&api)
}
