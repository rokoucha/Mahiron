package mirakurun

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/model"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestServiceEventCarriesMirakurunPayload(t *testing.T) {
	attemptedAt := int64(1000)
	succeededAt := int64(2000)
	logoID := int64(12)
	tsmfRelTs := uint8(1)
	hub := event.New()
	publisher := NewEventPublisher(hub)
	publisher.PublishServiceEvent(event.TypeUpdate, &model.Service{
		Key:  model.ServiceKey{ServiceID: 101, NetworkID: 1, StreamID: 10},
		Name: "NHK",
		Type: 1,
		Logo: &model.LogoRef{LogoID: uint16(logoID)},
	}, ServiceState{
		Channel:          &apigen.Channel{Type: "GR", Channel: "27", Name: apigen.NewOptString("NHK"), TsmfRelTs: apigen.NewOptInt(int(tsmfRelTs))},
		HasLogoData:      true,
		EPGLastAttemptAt: &attemptedAt,
		EPGLastSuccessAt: &succeededAt,
		EPGLastError:     "failed once",
	})

	events := hub.Log()
	if len(events) != 1 {
		t.Fatalf("events length = %d, want 1", len(events))
	}
	var decoded map[string]any
	if err := json.Unmarshal(events[0].Data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["id"] != float64(100101) || decoded["logoId"] != float64(logoID) ||
		decoded["hasLogoData"] != true || decoded["transportStreamId"] != float64(10) ||
		decoded["epgReady"] != true || decoded["epgUpdatedAt"] != float64(succeededAt) {
		t.Fatalf("service event data = %s", events[0].Data)
	}
	channel := decoded["channel"].(map[string]any)
	if channel["type"] != "GR" || channel["channel"] != "27" || channel["name"] != "NHK" || channel["tsmfRelTs"] != float64(tsmfRelTs) {
		t.Fatalf("service channel data = %s", events[0].Data)
	}
}

func TestProgramEventCarriesMirakurunPayload(t *testing.T) {
	startAt, duration := int64(1000), 1800
	expiresAt := int64(3000)
	pattern := 3
	hub := event.New()
	publisher := NewEventPublisher(hub)
	p := &model.Event{
		Key:         model.ServiceKey{NetworkID: 1, ServiceID: 101},
		EventID:     9,
		StartAt:     &startAt,
		DurationMS:  &duration,
		Name:        "program",
		Description: "description",
		Genres:      []model.Genre{{Lv1: 1, Lv2: 2, Un1: 3, Un2: 4}},
		Videos:      []model.VideoComponent{{Codec: model.VideoCodecMPEG2, Resolution: model.VideoResolution1080i, Aspect: model.VideoAspect16x9NoPanVector}},
		Audios:      []model.AudioComponent{{ComponentType: 3, Tag: 1, Main: true, SamplingHz: 48000, Languages: []string{"jpn"}}},
		Extended:    []model.ExtendedBlock{{Items: []model.ExtendedItem{{Name: "key", Text: "value"}}}},
		Related:     []model.RelatedEvent{{GroupType: model.EventGroupShared, NetworkID: 1, ServiceID: 101, EventID: 10}},
		Series:      &model.Series{ID: 1, Repeat: 2, Pattern: &pattern, ExpiresAt: &expiresAt, Episode: 4, LastEpisode: 5, Name: "series"},
	}
	publisher.PublishProgramEvent(event.TypeCreate, p)

	events := hub.Log()
	if len(events) != 1 {
		t.Fatalf("events length = %d, want 1", len(events))
	}
	// The stored payload must equal the shared conversion's output.
	api := ProgramToAPI(p)
	if want := MarshalProgram(&api); string(events[0].Data) != string(want) {
		t.Fatalf("program event data = %s, want %s", events[0].Data, want)
	}

	var decoded map[string]any
	if err := json.Unmarshal(events[0].Data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["id"] != float64(model.ProgramID(p.Key, 9)) || decoded["name"] != "program" {
		t.Fatalf("program event data = %s", events[0].Data)
	}
	if decoded["audios"].([]any)[0].(map[string]any)["langs"].([]any)[0] != "jpn" {
		t.Fatalf("program audio data = %s", events[0].Data)
	}
	if decoded["relatedItems"].([]any)[0].(map[string]any)["type"] != "shared" {
		t.Fatalf("program related item data = %s", events[0].Data)
	}
	if decoded["series"].(map[string]any)["expiresAt"] != float64(expiresAt) {
		t.Fatalf("program series data = %s", events[0].Data)
	}
	video := decoded["video"].(map[string]any)
	if video["type"] != "mpeg2" || video["resolution"] != "1080i" {
		t.Fatalf("program video data = %s", events[0].Data)
	}
	// Empty collections stay present as arrays while genres stays omitted.
	publisher.PublishProgramEvent(event.TypeCreate, &model.Event{
		Key:     model.ServiceKey{NetworkID: 1, ServiceID: 101},
		EventID: 9,
	})
	events = hub.Log()
	var minimal map[string]any
	if err := json.Unmarshal(events[1].Data, &minimal); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"audios", "relatedItems"} {
		items, ok := minimal[name].([]any)
		if !ok || len(items) != 0 {
			t.Fatalf("%s = %#v, want empty array", name, minimal[name])
		}
	}
	if _, ok := minimal["genres"]; ok {
		t.Fatalf("genres = %s, want omitted", events[1].Data)
	}
}

func TestProgramRemoveEventCarriesIDOnly(t *testing.T) {
	hub := event.New()
	publisher := NewEventPublisher(hub)
	publisher.PublishProgramRemove(event.TypeRemove, 42)
	events := hub.Log()
	if len(events) != 1 {
		t.Fatalf("events length = %d, want 1", len(events))
	}
	if got := strings.TrimSpace(string(events[0].Data)); got != `{"id":42}` {
		t.Fatalf("remove data = %s, want %s", got, `{"id":42}`)
	}
}
