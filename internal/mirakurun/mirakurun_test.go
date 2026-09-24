package mirakurun

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func fullProgram() *program.Program {
	componentTag := 16
	isMain := true
	samplingRate := 48000
	expiresAt := int64(1788609060000)
	return &program.Program{
		ID:          program.ProgramID(1, 101, 7),
		EventID:     7,
		ServiceID:   101,
		NetworkID:   1,
		StartAt:     1788609060000,
		Duration:    1800000,
		IsFree:      true,
		Name:        "大河ドラマ",
		Description: "解説文",
		Genres:      []program.Genre{{Lv1: 3, Lv2: 2, Un1: 15, Un2: 15}, {Lv1: 0, Lv2: 1, Un1: 15, Un2: 15}},
		Video:       &program.Video{StreamContent: 0x5, ComponentType: 0xB3},
		Audios: []program.Audio{
			{ComponentType: 3, ComponentTag: &componentTag, IsMain: &isMain, SamplingRate: &samplingRate, Langs: []string{"jpn", "eng"}},
			{ComponentType: 2},
		},
		Extended: map[string]string{
			"番組内容": "本文",
			"出演者":  "Foo",
		},
		RelatedItems: []program.RelatedItem{
			{Type: program.RelatedItemTypeShared, ServiceID: 101, EventID: 9},
		},
		Series: &program.Series{ID: 5, Repeat: 0, Pattern: 1, ExpiresAt: &expiresAt, Episode: 1, LastEpisode: 12, Name: "series"},
	}
}

func TestProgramRoundTrip(t *testing.T) {
	bareSeries := fullProgram()
	bareSeries.Series = &program.Series{ID: 5, Pattern: -1}
	for _, p := range []*program.Program{
		{ID: 1, EventID: 8, ServiceID: 101, NetworkID: 1},
		fullProgram(),
		bareSeries,
	} {
		api := ProgramToAPI(p)
		raw, err := api.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var decoded apigen.Program
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		// Empty collections travel as [] on the wire and come back nil,
		// matching how remote programs decode today.
		want := *p
		if len(want.Audios) == 0 {
			want.Audios = nil
		}
		if len(want.RelatedItems) == 0 {
			want.RelatedItems = nil
		}
		if got := ProgramFromAPI(&decoded); !reflect.DeepEqual(got, &want) {
			t.Fatalf("round trip = %#v, want %#v", got, &want)
		}
	}
}

func TestEncodeProgramSortsExtendedKeys(t *testing.T) {
	api := ProgramToAPI(fullProgram())
	first := MarshalProgram(&api)
	for i := 0; i < 50; i++ {
		if got := MarshalProgram(&api); !bytes.Equal(got, first) {
			t.Fatalf("unstable encoding:\n%s\n%s", first, got)
		}
	}
	// Extended keys must appear in sorted order in the encoded bytes.
	encoded := string(first)
	previous := -1
	for _, key := range []string{"出演者", "番組内容"} {
		keyJSON, _ := json.Marshal(key)
		pos := strings.Index(encoded, string(keyJSON)+":")
		if pos < 0 {
			t.Fatalf("key %q not found in %s", key, encoded)
		}
		if pos < previous {
			t.Fatalf("extended keys out of order in %s", encoded)
		}
		previous = pos
	}
}

func TestServiceToAPI(t *testing.T) {
	logoID := int64(5)
	lastSuccess := int64(1700000000000)
	svc := &service.Service{
		Id:                  "0000100101",
		ServiceId:           101,
		NetworkId:           1,
		TransportStreamId:   10,
		Name:                "NHK",
		Type:                1,
		EITScheduleFlag:     true,
		EITPresentFollowing: true,
		LogoId:              &logoID,
		HasLogoData:         true,
		RemoteControlKeyId:  3,
		ChannelType:         "GR",
		ChannelId:           "27",
		EPG:                 service.EPGStatus{LastSuccessAt: &lastSuccess},
	}
	channel := &config.ChannelConfig{Type: "GR", Channel: "27", Name: "NHK"}
	api := ServiceToAPI(svc, channel, true)
	if api.Name != "NHK" || int(api.ServiceId) != 101 {
		t.Fatalf("service = %#v", api)
	}
	channelValue, ok := api.Channel.Get()
	if !ok || channelValue.Channel != "27" {
		t.Fatalf("channel = %#v", api.Channel)
	}
	without := ServiceToAPI(svc, nil, false)
	if _, ok := without.Channel.Get(); ok {
		t.Fatalf("channel present without includeChannel")
	}
}

func TestScanServiceModelFromAPI(t *testing.T) {
	logoID := 12
	api := ServiceToAPI(&service.Service{
		ServiceId: 1024, NetworkId: 32736, TransportStreamId: 32736,
		Name: "remote service", Type: 1,
		EITScheduleFlag: true, EITPresentFollowing: true,
		LogoId:      func() *int64 { v := int64(logoID); return &v }(),
		HasLogoData: true, RemoteControlKeyId: 5,
	}, nil, false)
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
