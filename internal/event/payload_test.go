package event

import (
	"encoding/json"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
)

func TestServiceEventDataIncludesEPGAndChannel(t *testing.T) {
	attemptedAt := int64(1000)
	succeededAt := int64(2000)
	logoID := int64(12)
	tsmfRelTs := uint8(1)
	data := (&service.Service{
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
	}).EventData(&config.ChannelConfig{Type: "GR", Channel: "27", Name: "NHK", TsmfRelTs: &tsmfRelTs})

	if data["id"] != int64(100101) || data["logoId"] != logoID || data["hasLogoData"] != true ||
		data["transportStreamId"] != uint16(10) || data["epgReady"] != true || data["epgUpdatedAt"] != succeededAt {
		t.Fatalf("service event data = %#v", data)
	}
	channel := data["channel"].(map[string]any)
	if channel["type"] != "GR" || channel["channel"] != "27" || channel["name"] != "NHK" || channel["tsmfRelTs"] != tsmfRelTs {
		t.Fatalf("service channel data = %#v", channel)
	}
}

func TestProgramEventDataIncludesNestedFields(t *testing.T) {
	componentTag := 1
	isMain := true
	samplingRate := 48000
	networkID := uint16(1)
	expiresAt := int64(3000)
	data := (&program.Program{
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
	}).EventData()

	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["id"] != float64(program.ProgramID(1, 101, 9)) || decoded["name"] != "program" {
		t.Fatalf("program event data = %#v", decoded)
	}
	if decoded["audios"].([]any)[0].(map[string]any)["langs"].([]any)[0] != "jpn" {
		t.Fatalf("program audio data = %#v", decoded["audios"])
	}
	if decoded["relatedItems"].([]any)[0].(map[string]any)["type"] != "shared" {
		t.Fatalf("program related item data = %#v", decoded["relatedItems"])
	}
	if decoded["series"].(map[string]any)["expiresAt"] != float64(expiresAt) {
		t.Fatalf("program series data = %#v", decoded["series"])
	}
	video := decoded["video"].(map[string]any)
	if video["type"] != "mpeg2" || video["resolution"] != "1080i" {
		t.Fatalf("program video data = %#v", video)
	}
}

func TestProgramEventDataIncludesEmptyArrays(t *testing.T) {
	data := (&program.Program{
		ID:        program.ProgramID(1, 101, 9),
		NetworkID: 1,
		ServiceID: 101,
		EventID:   9,
		StartAt:   1000,
		Duration:  1800,
		IsFree:    true,
	}).EventData()

	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"audios", "relatedItems"} {
		items, ok := decoded[name].([]any)
		if !ok || len(items) != 0 {
			t.Fatalf("%s = %#v, want empty array", name, decoded[name])
		}
	}
	// Mirakurun omits genres for programs without genre information.
	if _, ok := decoded["genres"]; ok {
		t.Fatalf("genres = %#v, want omitted", decoded["genres"])
	}
}
