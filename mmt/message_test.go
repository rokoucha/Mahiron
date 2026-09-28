package mmt

import (
	"errors"
	"net/netip"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestParsePAMessageAndMPT(t *testing.T) {
	var body []byte
	body = append(body, 0xfc|0x01)                 // reserved, MPT_mode
	body = append(body, 4, 0x00, 0x00, 0x00, 0x65) // package ID
	body = append(body, 0x00, 0x00)                // MPT descriptors
	body = append(body, 2)                         // number_of_assets
	body = append(body, mptAsset("hev1", []byte{LocationTypeSameFlow, 0xF3, 0x00},
		mmttest.Descriptor(DescriptorTagMHStreamIdentifier, []byte{0x00, 0x00}))...)
	ipv4 := []byte{LocationTypeIPv4, 192, 0, 2, 1, 239, 0, 0, 1, 0x12, 0x34, 0xF3, 0x10}
	body = append(body, mptAsset("mp4a", ipv4)...)
	plt := mmttest.Table(0x80, 0, []byte{0})
	message := mmttest.PAMessage(3, mmttest.Table(TableIDMPT, 7, body), plt)

	pa, err := ParsePAMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	if pa.Version != 3 || len(pa.Tables) != 2 || pa.Tables[1].TableID() != 0x80 {
		t.Fatalf("PA message = %+v", pa)
	}
	mpt, err := ParseMPT(pa.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
	if mpt.Version != 7 || mpt.Mode != 1 || mpt.ServiceID() != 101 || len(mpt.Assets) != 2 {
		t.Fatalf("MPT = %+v", mpt)
	}
	video := mpt.Assets[0]
	if video.AssetType != "hev1" || !reflect.DeepEqual(video.Locations, []GeneralLocationInfo{{PacketID: 0xF300}}) {
		t.Fatalf("video asset = %+v", video)
	}
	if tag, err := ParseMHStreamIdentifierDescriptor(video.Descriptors[0]); err != nil || tag != 0 {
		t.Fatalf("component tag = %d, %v", tag, err)
	}
	wantAudio := GeneralLocationInfo{
		LocationType:    LocationTypeIPv4,
		Source:          netip.MustParseAddr("192.0.2.1"),
		Destination:     netip.MustParseAddr("239.0.0.1"),
		DestinationPort: 0x1234,
		PacketID:        0xF310,
	}
	if audio := mpt.Assets[1]; audio.AssetType != "mp4a" || !reflect.DeepEqual(audio.Locations, []GeneralLocationInfo{wantAudio}) {
		t.Fatalf("audio asset = %+v", audio)
	}
}

func TestParseMPTRejectsUnknownLocationType(t *testing.T) {
	body := append([]byte{0xfc, 0, 0, 0, 1}, mptAsset("hev1", []byte{0x7f, 0, 0})...)
	if _, err := ParseMPT(mmttest.Table(TableIDMPT, 0, body)); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("error = %v, want ErrInvalidSection", err)
	}
}

func mptAsset(assetType string, location []byte, descriptors ...[]byte) []byte {
	b := []byte{0x00, 0, 0, 0, 0, 4, 0, 0, 0, 1} // identifier_type, scheme, asset ID
	b = append(b, assetType...)
	b = append(b, 0xfe, 1) // no clock relation, location_count
	b = append(b, location...)
	var d []byte
	for _, desc := range descriptors {
		d = append(d, desc...)
	}
	b = append(b, byte(len(d)>>8), byte(len(d)))
	return append(b, d...)
}

func TestMessageSection(t *testing.T) {
	section := mmttest.Section(TableIDMHSDTActual, 0xB110, 0, 0, 0, []byte{0x00, 0x0b, 0xff})
	got, err := Message(mmttest.M2SectionMessage(0, section)).Section()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]byte(got), section) || !got.ValidateCRC() {
		t.Fatalf("section = %x, want %x", got, section)
	}

	if _, err := Message(mmttest.PAMessage(0)).Section(); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("PA message error = %v, want ErrInvalidSection", err)
	}
	truncated := mmttest.M2SectionMessage(0, section[:len(section)-1])
	if _, err := Message(truncated).Section(); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("truncated section error = %v, want ErrInvalidSection", err)
	}
}

func TestParsePLT(t *testing.T) {
	src := netip.MustParseAddr("2001:db8::1")
	dst := netip.MustParseAddr("ff3e::1")
	var body []byte
	body = append(body, 1, 2, 0x00, 0x65, LocationTypeSameFlow, 0xFF, 0x02) // one package on packet_id 0xFF02
	body = append(body, 1, 0x00, 0x00, 0x00, 0x07, LocationTypeIPv6)        // one IP delivery
	body = append(body, src.AsSlice()...)
	body = append(body, dst.AsSlice()...)
	descriptor := mmttest.Descriptor(DescriptorTagMHContent, []byte{0x81, 0xff})
	body = append(body, 0x12, 0x34, 0x00, byte(len(descriptor)))
	body = append(body, descriptor...)
	pa, err := ParsePAMessage(mmttest.PAMessage(0, mmttest.Table(TableIDPLT, 0x22, body)))
	if err != nil {
		t.Fatal(err)
	}
	plt, err := ParsePLT(pa.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
	if plt.Version != 0x22 || len(plt.Packages) != 1 || plt.Packages[0].ServiceID() != 101 || plt.Packages[0].Location != (GeneralLocationInfo{PacketID: 0xFF02}) {
		t.Fatalf("PLT = %+v", plt)
	}
	want := []PLTIPDelivery{{
		TransportFileID: 7,
		LocationType:    LocationTypeIPv6,
		Source:          src,
		Destination:     dst,
		DestinationPort: 0x1234,
		Descriptors:     []Descriptor{{Tag: DescriptorTagMHContent, Data: []byte{0x81, 0xff}}},
	}}
	if !reflect.DeepEqual(plt.IPDeliveries, want) {
		t.Fatalf("IP deliveries = %+v, want %+v", plt.IPDeliveries, want)
	}
}

func TestParsePLTRejectsInvalidTables(t *testing.T) {
	tables := map[string][]byte{
		"MPT":                   mmttest.Table(TableIDMPT, 0, []byte{0, 0}),
		"unknown location type": mmttest.Table(TableIDPLT, 0, []byte{1, 2, 0x00, 0x65, 0x7f, 0x00}),
		"MPEG-2 TS delivery":    mmttest.Table(TableIDPLT, 0, []byte{0, 1, 0, 0, 0, 1, LocationTypeMPEG2TS, 0, 0}),
		"truncated":             mmttest.Table(TableIDPLT, 0, []byte{1, 2, 0x00, 0x65, LocationTypeSameFlow, 0xFF}),
	}
	for name, table := range tables {
		if _, err := ParsePLT(table); !errors.Is(err, ErrInvalidSection) {
			t.Errorf("%s: error = %v, want ErrInvalidSection", name, err)
		}
	}
}
