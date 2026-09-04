package remote

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	mahirondb "github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/program"
)

// TestRemoteProgramRoundTripsThroughStore guards the normalization that lets
// ProgramManager.UpsertPrograms skip writing a program that hasn't actually
// changed: it compares by reflect.DeepEqual, so a program converted from a
// Mirakurun-style remote event must come out byte-for-byte identical to the
// same program read back from the database, in particular for empty
// collections (Genres/Audios/Extended/RelatedItems), which the database
// normalizes to nil regardless of whether the wire JSON omitted the field or
// spelled it out as an empty array/object.
func TestRemoteProgramRoundTripsThroughStore(t *testing.T) {
	ctx := context.Background()
	database, err := mahirondb.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := program.NewSQLiteStore(database)

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
			var remote remoteProgram
			if err := json.Unmarshal([]byte(tc.json), &remote); err != nil {
				t.Fatal(err)
			}
			converted := remote.Program()
			if err := store.UpsertAll(ctx, []*program.Program{converted}); err != nil {
				t.Fatal(err)
			}
			stored, ok, err := store.Get(ctx, converted.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("program not stored")
			}

			var remoteAgain remoteProgram
			if err := json.Unmarshal([]byte(tc.json), &remoteAgain); err != nil {
				t.Fatal(err)
			}
			wanted := remoteAgain.Program()
			if !reflect.DeepEqual(wanted, stored) {
				t.Fatalf("round trip mismatch:\n  converted = %#v\n  stored    = %#v", wanted, stored)
			}
		})
	}
}
