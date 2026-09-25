package mirakurun

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func fullEvent() model.Event {
	startAt, duration := int64(1788609060000), 1800000
	expiresAt := int64(1788609060000)
	pattern := 1
	return model.Event{
		Key:         model.ServiceKey{NetworkID: 1, ServiceID: 101},
		EventID:     7,
		StartAt:     &startAt,
		DurationMS:  &duration,
		Name:        "大河ドラマ",
		Description: "解説文",
		Genres:      []model.Genre{{Lv1: 3, Lv2: 2, Un1: 15, Un2: 15}, {Lv1: 0, Lv2: 1, Un1: 15, Un2: 15}},
		Videos:      []model.VideoComponent{{Codec: model.VideoCodecH264, Resolution: model.VideoResolution1080i, Aspect: model.VideoAspect16x9NoPanVector}},
		Audios: []model.AudioComponent{
			{ComponentType: 3, Tag: 16, Main: true, SamplingHz: 48000, Languages: []string{"jpn", "eng"}},
			{ComponentType: 2, Languages: []string{}},
		},
		Extended: []model.ExtendedBlock{{Items: []model.ExtendedItem{
			{Name: "番組内容", Text: "本文"},
			{Name: "出演者", Text: "Foo"},
		}}},
		Related: []model.RelatedEvent{{GroupType: model.EventGroupShared, ServiceID: 101, EventID: 9}},
		Series:  &model.Series{ID: 5, Repeat: 0, Pattern: &pattern, ExpiresAt: &expiresAt, Episode: 1, LastEpisode: 12, Name: "series"},
	}
}

// TestProgramRoundTrip checks that a remote Mahiron's program comes back as
// the event it was built from, for the values the Mirakurun shape carries.
func TestProgramRoundTrip(t *testing.T) {
	bareSeries := fullEvent()
	bareSeries.Series = &model.Series{ID: 5}
	for _, e := range []model.Event{
		{Key: model.ServiceKey{NetworkID: 1, ServiceID: 101}, EventID: 8},
		fullEvent(),
		bareSeries,
	} {
		api := ProgramToAPI(&e)
		raw, err := api.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var decoded apigen.Program
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if got := EventFromAPI(&decoded); !reflect.DeepEqual(got, e) {
			t.Fatalf("round trip = %#v, want %#v", got, e)
		}
	}
}

// TestProgramToAPIKeepsExtendedOrder pins that extended items come out in
// broadcast order, in every encoding, and that a heading repeated in another
// language keeps its first text.
func TestProgramToAPIKeepsExtendedOrder(t *testing.T) {
	e := fullEvent()
	e.Extended = append(e.Extended, model.ExtendedBlock{Language: "eng", Items: []model.ExtendedItem{
		{Name: "番組内容", Text: "english"},
		{Name: "あらすじ", Text: "later"},
	}})
	api := ProgramToAPI(&e)
	want := `"extended":{"番組内容":"本文","出演者":"Foo","あらすじ":"later"}`
	for i := 0; i < 20; i++ {
		raw, err := api.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), want) {
			t.Fatalf("encoded = %s, want %s", raw, want)
		}
	}
	if got := string(MarshalProgram(&api)); !strings.Contains(got, want) {
		t.Fatalf("MarshalProgram = %s, want %s", got, want)
	}
}

func TestServiceToAPI(t *testing.T) {
	lastSuccess := int64(1700000000000)
	svc := &model.Service{
		Key:              model.ServiceKey{ServiceID: 101, NetworkID: 1, StreamID: 10},
		Name:             "NHK",
		Type:             1,
		EITSchedule:      true,
		EITPresentFollow: true,
		RemoteControlKey: new(uint8(3)),
		Logo:             &model.LogoRef{LogoID: 5},
	}
	api := ServiceToAPI(svc, ServiceState{
		Channel:          &apigen.Channel{Type: "GR", Channel: "27"},
		HasLogoData:      true,
		EPGLastSuccessAt: &lastSuccess,
	})
	if api.Name != "NHK" || int(api.ServiceId) != 101 || int(api.ID) != 100101 || api.RemoteControlKeyId.Value != 3 ||
		api.LogoId.Value != 5 || !api.HasLogoData.Value || !api.EpgReady.Value {
		t.Fatalf("service = %#v", api)
	}
	channelValue, ok := api.Channel.Get()
	if !ok || channelValue.Channel != "27" {
		t.Fatalf("channel = %#v", api.Channel)
	}

	// Without a channel, remote control key or logo, the key is written as
	// 0 and the channel and logo ID stay absent.
	bare := ServiceToAPI(&model.Service{Key: svc.Key, Name: "bare"}, ServiceState{})
	if _, ok := bare.Channel.Get(); ok {
		t.Fatalf("channel present without a channel")
	}
	if key, ok := bare.RemoteControlKeyId.Get(); !ok || key != 0 {
		t.Fatalf("remoteControlKeyId = %#v, want 0", bare.RemoteControlKeyId)
	}
	if _, ok := bare.LogoId.Get(); ok {
		t.Fatalf("logoId present without a logo")
	}
}

func TestScanServiceModelFromAPI(t *testing.T) {
	api := ServiceToAPI(&model.Service{
		Key:              model.ServiceKey{ServiceID: 1024, NetworkID: 32736, StreamID: 32736},
		Name:             "remote service",
		Type:             1,
		EITSchedule:      true,
		EITPresentFollow: true,
		RemoteControlKey: new(uint8(5)),
		Logo:             &model.LogoRef{LogoID: 12},
	}, ServiceState{HasLogoData: true})
	got := ScanServiceModelFromAPI(&api)
	if got.Key != (model.ServiceKey{NetworkID: 32736, StreamID: 32736, ServiceID: 1024}) || got.Name != "remote service" {
		t.Fatalf("scan = %#v", got)
	}
	if got.Logo == nil || got.Logo.LogoID != 12 || got.Logo.Version == nil || *got.Logo.Version != 0 ||
		got.Logo.DownloadDataID == nil || *got.Logo.DownloadDataID != 1024 {
		t.Fatalf("logo = %#v", got.Logo)
	}
	if got.RemoteControlKey == nil || *got.RemoteControlKey != 5 {
		t.Fatalf("remoteControlKey = %#v", got.RemoteControlKey)
	}

	// A logo ID without logo data does not count, and absent EIT flags
	// default to true.
	bare := apigen.Service{ServiceId: 101, NetworkId: 4, Name: "bare", Type: 1}
	bare.LogoId = apigen.NewOptInt(13)
	bare.HasLogoData = apigen.NewOptBool(false)
	got = ScanServiceModelFromAPI(&bare)
	if got.Logo != nil {
		t.Fatalf("logo = %#v", got.Logo)
	}
	if !got.EITSchedule || !got.EITPresentFollow {
		t.Fatalf("EIT flags = %v/%v, want true/true", got.EITSchedule, got.EITPresentFollow)
	}
}
