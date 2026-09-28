package mmt

import (
	"errors"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestParseDescriptorsUsesLengthWidthOfTag(t *testing.T) {
	long := make([]byte, 300)
	b := append(mmttest.Descriptor(DescriptorTagMHShortEvent, long), mmttest.Descriptor(DescriptorTagMHContent, []byte{0x81, 0xff})...)
	got, err := parseDescriptors(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Tag != DescriptorTagMHShortEvent || len(got[0].Data) != 300 || got[1].Tag != DescriptorTagMHContent {
		t.Fatalf("descriptors = %+v", got)
	}
	if _, err := parseDescriptors(b[:len(b)-1]); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("truncated error = %v, want ErrInvalidSection", err)
	}
}

func TestJoinExtendedEventJoinsSplitItemsAndText(t *testing.T) {
	// "出演者" is split inside a UTF-8 sequence between the descriptors.
	item := []byte("出演者")
	first := parseExtendedEvent(t, extendedEvent(0, 1, [][2][]byte{{[]byte("番組内容"), []byte("本文")}, {[]byte("出演"), item[:4]}}, []byte("テ")[:2]))
	second := parseExtendedEvent(t, extendedEvent(1, 1, [][2][]byte{{nil, item[4:]}}, []byte("テ")[2:]))
	got := JoinExtendedEvent([]*MHExtendedEventDescriptor{second, first})
	want := ExtendedEvent{
		Language: "jpn",
		Items:    []ExtendedEventItem{{Description: "番組内容", Item: "本文"}, {Description: "出演", Item: "出演者"}},
		Text:     "テ",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extended event = %+v, want %+v", got, want)
	}
}

func extendedEvent(number, last byte, items [][2][]byte, text []byte) Descriptor {
	var loop []byte
	for _, item := range items {
		loop = append(loop, byte(len(item[0])))
		loop = append(loop, item[0]...)
		loop = append(loop, byte(len(item[1])>>8), byte(len(item[1])))
		loop = append(loop, item[1]...)
	}
	b := append([]byte{number<<4 | last, 'j', 'p', 'n', byte(len(loop) >> 8), byte(len(loop))}, loop...)
	b = append(b, byte(len(text)>>8), byte(len(text)))
	return Descriptor{Tag: DescriptorTagMHExtendedEvent, Data: append(b, text...)}
}

func parseExtendedEvent(t *testing.T, d Descriptor) *MHExtendedEventDescriptor {
	t.Helper()
	out, err := ParseMHExtendedEventDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestParseVideoComponentDescriptor(t *testing.T) {
	d := Descriptor{Tag: DescriptorTagVideoComponent, Data: []byte{0x73, 0x88, 0x00, 0x01, 0x5f, 'j', 'p', 'n', 'H', 'L', 'G'}}
	got, err := ParseVideoComponentDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	want := VideoComponentDescriptor{Resolution: 7, AspectRatio: 3, Progressive: true, FrameRate: 8, ComponentTag: 1, TransferCharacteristics: 5, Language: "jpn", Text: "HLG"}
	if *got != want {
		t.Fatalf("video component = %+v, want %+v", got, want)
	}
}

func TestParseMHAudioComponentDescriptor(t *testing.T) {
	d := Descriptor{Tag: DescriptorTagMHAudioComponent, Data: []byte{0xf3, 0x02, 0x00, 0x11, 0x11, 0xff, 0xde, 'j', 'p', 'n', 'e', 'n', 'g', 'x'}}
	got, err := ParseMHAudioComponentDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	want := MHAudioComponentDescriptor{
		StreamContent: 3, ComponentType: 2, ComponentTag: 0x11, StreamType: 0x11, SimulcastGroupTag: 0xff,
		ESMultiLingual: true, MainComponent: true, QualityIndicator: 1, SamplingRate: 7,
		Language: "jpn", Language2: "eng", Text: "x",
	}
	if *got != want || got.SamplingRateHz() != 48000 {
		t.Fatalf("audio component = %+v, want %+v", got, want)
	}
	if _, err := ParseMHAudioComponentDescriptor(Descriptor{Tag: DescriptorTagMHAudioComponent, Data: d.Data[:9]}); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("truncated error = %v, want ErrInvalidSection", err)
	}
}

func TestParseMHEventGroupDescriptor(t *testing.T) {
	d := Descriptor{Tag: DescriptorTagMHEventGroup, Data: []byte{0x41, 0x00, 0x65, 0x12, 0x34, 0x00, 0x0b, 0xb0, 0xe0, 0x00, 0x66, 0x56, 0x78}}
	got, err := ParseMHEventGroupDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	want := &MHEventGroupDescriptor{
		GroupType:          4,
		Events:             []GroupEvent{{ServiceID: 101, EventID: 0x1234}},
		OtherNetworkEvents: []GroupEvent{{OriginalNetworkID: 0x000b, TLVStreamID: 0xb0e0, ServiceID: 102, EventID: 0x5678}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event group = %+v, want %+v", got, want)
	}
}

func TestParseMHSeriesDescriptor(t *testing.T) {
	d := Descriptor{Tag: DescriptorTagMHSeries, Data: []byte{0x12, 0x34, 0x13, 0xee, 0x33, 0x00, 0xc0, 0x0d, 's'}}
	got, err := ParseMHSeriesDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	want := MHSeriesDescriptor{SeriesID: 0x1234, RepeatLabel: 1, ProgramPattern: 1, ExpireDateValid: true, ExpireDate: 0xee33, EpisodeNumber: 12, LastEpisodeNumber: 13, SeriesName: "s"}
	if *got != want {
		t.Fatalf("series = %+v, want %+v", got, want)
	}
}

func TestParseMHParentalRatingDescriptor(t *testing.T) {
	got, err := ParseMHParentalRatingDescriptor(Descriptor{Tag: DescriptorTagMHParentalRating, Data: []byte("JPN\x05USA\x00")})
	if err != nil {
		t.Fatal(err)
	}
	want := []ParentalRating{{CountryCode: "JPN", Rating: 5}, {CountryCode: "USA", Rating: 0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ratings = %+v, want %+v", got, want)
	}
}
