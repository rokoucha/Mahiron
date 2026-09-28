package mmt

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestParseTLVNIT(t *testing.T) {
	var body []byte
	body = append(body, mmttest.Loop(
		mmttest.TLVDescriptor(TLVDescriptorTagNetworkName, []byte("高度ＢＳデジタル放送")),
		mmttest.TLVDescriptor(TLVDescriptorTagRemoteControlKey, []byte{1, 1, 0x00, 0x65, 0xff, 0xff}),
	)...)
	stream := append([]byte{0xB0, 0xE0, 0x00, 0x0B}, mmttest.Loop(mmttest.TLVDescriptor(TLVDescriptorTagServiceList, []byte{0x00, 0x66, 0x01}))...)
	body = append(body, mmttest.Loop(stream)...)
	nit, err := ParseTLVNIT(mmttest.Section(TableIDTLVNITActual, 0x000B, 7, 0, 0, body))
	if err != nil {
		t.Fatal(err)
	}
	if nit.NetworkID != 0x000B || nit.VersionNumber != 7 || len(nit.NetworkDescriptors) != 2 || string(nit.NetworkDescriptors[0].Data) != "高度ＢＳデジタル放送" {
		t.Fatalf("TLV-NIT = %+v", nit)
	}
	keys, err := ParseRemoteControlKeyDescriptor(nit.NetworkDescriptors[1])
	if err != nil || !reflect.DeepEqual(keys, []RemoteControlKey{{RemoteControlKeyID: 1, ServiceID: 101}}) {
		t.Fatalf("remote control keys = %+v, %v", keys, err)
	}
	if len(nit.TLVStreams) != 1 || nit.TLVStreams[0].TLVStreamID != 0xB0E0 || nit.TLVStreams[0].OriginalNetworkID != 0x000B {
		t.Fatalf("TLV streams = %+v", nit.TLVStreams)
	}
	services, err := ParseServiceListDescriptor(nit.TLVStreams[0].Descriptors[0])
	if err != nil || !reflect.DeepEqual(services, []ServiceListEntry{{ServiceID: 102, ServiceType: 0x01}}) {
		t.Fatalf("services = %+v, %v", services, err)
	}
}

func TestParseTLVNITRejectsInvalidSections(t *testing.T) {
	valid := mmttest.Section(TableIDTLVNITActual, 0x000B, 0, 0, 0, append(mmttest.Loop(), mmttest.Loop()...))
	if _, err := ParseTLVNIT(valid); err != nil {
		t.Fatal(err)
	}
	brokenCRC := append(Section(nil), valid...)
	brokenCRC[len(brokenCRC)-1] ^= 0xff
	amt := mmttest.Section(TableIDAMT, 0, 0, 0, 0, append(mmttest.Loop(), mmttest.Loop()...))
	longLoop := mmttest.Section(TableIDTLVNITActual, 0x000B, 0, 0, 0, append(mmttest.Loop(), 0xf0, 0x05))
	for name, s := range map[string][]byte{"bad CRC": brokenCRC, "AMT": amt, "loop too long": longLoop} {
		if _, err := ParseTLVNIT(s); !errors.Is(err, ErrInvalidSection) {
			t.Errorf("%s: error = %v, want ErrInvalidSection", name, err)
		}
	}
}

func TestParseMHEIT(t *testing.T) {
	start := time.Date(2026, 9, 23, 21, 57, 0, 0, jst)
	shortEvent := mmttest.Descriptor(DescriptorTagMHShortEvent, append(append([]byte{'j', 'p', 'n', byte(len("番組"))}, "番組"...), 0x00, 0x03, 'a', 'b', 'c'))
	content := mmttest.Descriptor(DescriptorTagMHContent, []byte{0x81, 0xff})
	var body []byte
	body = append(body, 0xB1, 0x10, 0x00, 0x0B, 0x01, TableIDMHEITPF) // tlv_stream_id, original_network_id, segment_last, last_table_id
	body = append(body, mhEITEvent(0x1234, mmttest.MJDTime(start), mmttest.BCDDuration(90*time.Minute), 4, shortEvent, content)...)
	body = append(body, mhEITEvent(0x1235, []byte{0xff, 0xff, 0xff, 0xff, 0xff}, []byte{0xff, 0xff, 0xff}, 1)...)
	eit, err := ParseMHEIT(mmttest.Section(TableIDMHEITPF, 101, 3, 0, 1, body))
	if err != nil {
		t.Fatal(err)
	}
	if eit.ServiceID != 101 || eit.TLVStreamID != 0xB110 || eit.OriginalNetworkID != 0x000B || eit.VersionNumber != 3 || eit.LastSectionNumber != 1 || eit.LastTableID != TableIDMHEITPF || len(eit.Events) != 2 {
		t.Fatalf("MH-EIT = %+v", eit)
	}
	event := eit.Events[0]
	if event.EventID != 0x1234 || !event.StartTime.Equal(start) || event.Duration != 90*time.Minute || event.RunningStatus != 4 || len(event.Descriptors) != 2 {
		t.Fatalf("event = %+v", event)
	}
	short, err := ParseMHShortEventDescriptor(event.Descriptors[0])
	if err != nil || *short != (MHShortEventDescriptor{Language: "jpn", EventName: "番組", Text: "abc"}) {
		t.Fatalf("short event = %+v, %v", short, err)
	}
	genres, err := ParseMHContentDescriptor(event.Descriptors[1])
	if err != nil || !reflect.DeepEqual(genres, []ContentGenre{{Level1: 8, Level2: 1, User1: 0xf, User2: 0xf}}) {
		t.Fatalf("genres = %+v, %v", genres, err)
	}
	if undefined := eit.Events[1]; !undefined.StartTime.IsZero() || undefined.Duration != 0 {
		t.Fatalf("undefined event = %+v", undefined)
	}
}

func TestParseMHEITRejectsInvalidSections(t *testing.T) {
	header := []byte{0xB1, 0x10, 0x00, 0x0B, 0x00, TableIDMHEITPF}
	start := mmttest.MJDTime(time.Date(2026, 9, 23, 0, 0, 0, 0, jst))
	brokenBCD := append(append([]byte(nil), header...), mhEITEvent(1, start, []byte{0x00, 0x6a, 0x00}, 0)...)
	brokenLoop := append(append([]byte(nil), header...), mhEITEvent(1, start, mmttest.BCDDuration(time.Hour), 0)...)
	brokenLoop[len(brokenLoop)-1] = 1
	brokenDescriptor := append(append([]byte(nil), header...), mhEITEvent(1, start, mmttest.BCDDuration(time.Hour), 0, []byte{0xF0, 0x01, 0x00, 0x05})...)
	sections := map[string][]byte{
		"TS EIT":            mmttest.Section(0x4E, 101, 0, 0, 0, header),
		"broken BCD":        mmttest.Section(TableIDMHEITPF, 101, 0, 0, 0, brokenBCD),
		"loop too long":     mmttest.Section(TableIDMHEITPF, 101, 0, 0, 0, brokenLoop),
		"broken descriptor": mmttest.Section(TableIDMHEITPF, 101, 0, 0, 0, brokenDescriptor),
	}
	for name, s := range sections {
		if _, err := ParseMHEIT(s); !errors.Is(err, ErrInvalidSection) {
			t.Errorf("%s: error = %v, want ErrInvalidSection", name, err)
		}
	}
}

func mhEITEvent(eventID uint16, start, duration []byte, runningStatus byte, descriptors ...[]byte) []byte {
	b := append([]byte{byte(eventID >> 8), byte(eventID)}, start...)
	b = append(b, duration...)
	loop := mmttest.Loop(descriptors...)
	loop[0] = runningStatus<<5 | loop[0]&0x0f
	return append(b, loop...)
}

func TestParseMHSDT(t *testing.T) {
	provider, name := "ＮＨＫ", "ＮＨＫ　ＢＳ８Ｋ"
	service := mmttest.Descriptor(DescriptorTagMHService, append(append(append([]byte{0x01, byte(len(provider))}, provider...), byte(len(name))), name...))
	loop := mmttest.Loop(service)
	loop[0] = 4<<5 | loop[0]&0x0f
	body := append([]byte{0x00, 0x0B, 0xff, 0x00, 0x66, 0xe3}, loop...)
	sdt, err := ParseMHSDT(mmttest.Section(TableIDMHSDTActual, 0xB0E0, 1, 0, 0, body))
	if err != nil {
		t.Fatal(err)
	}
	if sdt.TLVStreamID != 0xB0E0 || sdt.OriginalNetworkID != 0x000B || len(sdt.Services) != 1 {
		t.Fatalf("MH-SDT = %+v", sdt)
	}
	s := sdt.Services[0]
	if s.ServiceID != 102 || !s.EITScheduleFlag || !s.EITPresentFollowing || s.RunningStatus != 4 || s.FreeCAMode {
		t.Fatalf("service = %+v", s)
	}
	d, err := ParseMHServiceDescriptor(s.Descriptors[0])
	if err != nil || *d != (MHServiceDescriptor{ServiceType: 0x01, ServiceProviderName: provider, ServiceName: name}) {
		t.Fatalf("service descriptor = %+v, %v", d, err)
	}
}

func TestParseMHTOT(t *testing.T) {
	now := time.Date(2026, 9, 23, 21, 11, 42, 0, jst)
	section := mmttest.ShortSection(TableIDMHTOT, append(mmttest.MJDTime(now), mmttest.Loop()...))
	tot, err := ParseMHTOT(section)
	if err != nil {
		t.Fatal(err)
	}
	if !tot.JSTTime.Equal(now) || len(tot.Descriptors) != 0 {
		t.Fatalf("MH-TOT = %+v", tot)
	}
	brokenCRC := append(Section(nil), section...)
	brokenCRC[len(brokenCRC)-1] ^= 0xff
	if _, err := ParseMHTOT(brokenCRC); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("broken CRC error = %v, want ErrInvalidSection", err)
	}
}
