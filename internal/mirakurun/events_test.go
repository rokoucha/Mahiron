package mirakurun

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
)

func TestServiceEventCarriesMirakurunPayload(t *testing.T) {
	attemptedAt := int64(1000)
	succeededAt := int64(2000)
	logoID := int64(12)
	tsmfRelTs := uint8(1)
	hub := event.New()
	publisher := NewEventPublisher(hub)
	publisher.PublishServiceEvent(event.TypeUpdate, &service.Service{
		ServiceId:         101,
		NetworkId:         1,
		TransportStreamId: 10,
		Name:              "NHK",
		Type:              1,
		LogoId:            &logoID,
		HasLogoData:       true,
		EPG: service.EPGStatus{
			LastAttemptAt: &attemptedAt,
			LastSuccessAt: &succeededAt,
			LastError:     "failed once",
		},
		ChannelType: "GR",
		ChannelId:   "27",
	}, &config.ChannelConfig{Type: "GR", Channel: "27", Name: "NHK", TsmfRelTs: &tsmfRelTs})

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
	componentTag := 1
	isMain := true
	samplingRate := 48000
	networkID := uint16(1)
	expiresAt := int64(3000)
	hub := event.New()
	publisher := NewEventPublisher(hub)
	p := &program.Program{
		ID:          program.ProgramID(1, 101, 9),
		NetworkID:   1,
		ServiceID:   101,
		EventID:     9,
		StartAt:     1000,
		Duration:    1800,
		IsFree:      true,
		Name:        "program",
		Description: "description",
		Genres:      []program.Genre{{Lv1: 1, Lv2: 2, Un1: 3, Un2: 4}},
		Video:       &program.Video{StreamContent: 1, ComponentType: 179},
		Audios: []program.Audio{{
			ComponentType: 3,
			ComponentTag:  &componentTag,
			IsMain:        &isMain,
			SamplingRate:  &samplingRate,
			Langs:         []string{"jpn"},
		}},
		Extended: map[string]string{"key": "value"},
		RelatedItems: []program.RelatedItem{{
			Type:      program.RelatedItemTypeShared,
			NetworkID: &networkID,
			ServiceID: 101,
			EventID:   10,
		}},
		Series: &program.Series{
			ID:          1,
			Repeat:      2,
			Pattern:     3,
			ExpiresAt:   &expiresAt,
			Episode:     4,
			LastEpisode: 5,
			Name:        "series",
		},
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
	if decoded["id"] != float64(program.ProgramID(1, 101, 9)) || decoded["name"] != "program" {
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
	publisher.PublishProgramEvent(event.TypeCreate, &program.Program{
		ID:        program.ProgramID(1, 101, 9),
		NetworkID: 1,
		ServiceID: 101,
		EventID:   9,
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
