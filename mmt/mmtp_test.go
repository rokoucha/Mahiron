package mmt

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestParseMMTPPacket(t *testing.T) {
	counter := uint32(0x01020304)
	b := mmttest.MMTP{
		PayloadType:    MMTPPayloadTypeMPU,
		PacketID:       0xF300,
		Timestamp:      0x11223344,
		SequenceNumber: 0x55667788,
		RAP:            true,
		PacketCounter:  &counter,
		ExtensionType:  0x0000,
		Extension:      []byte{0x80, 0x01, 0x00, 0x01, 0x00},
		Payload:        []byte("payload"),
	}.Bytes()
	got, err := ParseMMTPPacket(b)
	if err != nil {
		t.Fatal(err)
	}
	want := &MMTPPacket{
		RAP:                  true,
		PayloadType:          MMTPPayloadTypeMPU,
		PacketID:             0xF300,
		Timestamp:            0x11223344,
		PacketSequenceNumber: 0x55667788,
		HasPacketCounter:     true,
		PacketCounter:        counter,
		Extension:            &MMTPHeaderExtension{Type: 0x0000, Data: []byte{0x80, 0x01, 0x00, 0x01, 0x00}},
		Payload:              []byte("payload"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("packet = %+v, want %+v", got, want)
	}
}

func TestParseMMTPPacketRejectsTruncatedExtension(t *testing.T) {
	b := mmttest.MMTP{PacketID: 1, Extension: []byte{1, 2, 3}}.Bytes()
	if _, err := ParseMMTPPacket(b[:len(b)-1]); !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("error = %v, want ErrInvalidPacket", err)
	}
}

func TestMessageAssemblerJoinsFragments(t *testing.T) {
	message := mmttest.M2SectionMessage(0, mmttest.Section(TableIDMHEITPF, 101, 0, 0, 0, bytes.Repeat([]byte{0xab}, 100)))
	packets := mmttest.SignalingPackets(PacketIDMHEIT, 10, message, 40)
	if len(packets) < 3 {
		t.Fatalf("packets = %d, want at least 3 fragments", len(packets))
	}
	got := feedMessages(t, packets)
	if !reflect.DeepEqual(got, []Message{message}) {
		t.Fatalf("messages = %x, want %x", got, message)
	}
}

func TestMessageAssemblerDropsMessageWithMissingFragment(t *testing.T) {
	message := mmttest.M2SectionMessage(0, mmttest.Section(TableIDMHEITPF, 101, 0, 0, 0, bytes.Repeat([]byte{0xab}, 100)))
	packets := mmttest.SignalingPackets(PacketIDMHEIT, 10, message, 40)
	complete := mmttest.SignalingPackets(PacketIDMHEIT, 20, message, len(message))
	got := feedMessages(t, append(append([][]byte{packets[0]}, packets[2:]...), complete...))
	if !reflect.DeepEqual(got, []Message{message}) {
		t.Fatalf("messages = %x, want only the complete message", got)
	}
}

func TestMessageAssemblerDropsOversizedMessage(t *testing.T) {
	const fragmentSize = 60000
	fragment := make([]byte, fragmentSize)
	var packets [][]byte
	n := maxMessageSize/fragmentSize + 2
	for i := range n {
		indicator := byte(FragmentMiddle)
		switch i {
		case 0:
			indicator = FragmentFirst
		case n - 1:
			indicator = FragmentLast
		}
		payload := mmttest.SignalingPayload(indicator, byte(n-1-i), fragment)
		packets = append(packets, mmttest.MMTP{PayloadType: MMTPPayloadTypeSignaling, PacketID: 0x9000, SequenceNumber: uint32(i), Payload: payload}.Bytes())
	}
	var a MessageAssembler
	for _, b := range packets {
		p, err := ParseMMTPPacket(b)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := a.Feed(IPDataFlow{}, p); err != nil || got != nil {
			t.Fatalf("messages = %x, %v, want none", got, err)
		}
		if pending := a.pending[PacketKey{PacketID: 0x9000}]; pending != nil && len(pending.data) > maxMessageSize {
			t.Fatalf("pending message grew to %d bytes", len(pending.data))
		}
	}
	if len(a.pending) != 0 {
		t.Fatalf("pending = %d messages, want the oversized one dropped", len(a.pending))
	}
}

func TestMessageAssemblerSplitsAggregatedMessages(t *testing.T) {
	first := mmttest.M2ShortSectionMessage(0, mmttest.ShortSection(TableIDMHTOT, []byte{1, 2, 3}))
	second := mmttest.M2ShortSectionMessage(1, mmttest.ShortSection(TableIDMHTOT, []byte{4, 5}))
	packet := mmttest.MMTP{PayloadType: MMTPPayloadTypeSignaling, PacketID: PacketIDMHTOT, Payload: mmttest.AggregatedSignalingPayload(first, second)}.Bytes()
	got := feedMessages(t, [][]byte{packet})
	if !reflect.DeepEqual(got, []Message{first, second}) {
		t.Fatalf("messages = %x", got)
	}
}

func feedMessages(t *testing.T, packets [][]byte) []Message {
	t.Helper()
	var a MessageAssembler
	var messages []Message
	for _, b := range packets {
		p, err := ParseMMTPPacket(b)
		if err != nil {
			t.Fatal(err)
		}
		got, err := a.Feed(IPDataFlow{}, p)
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, got...)
	}
	return messages
}
