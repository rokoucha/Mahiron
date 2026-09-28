package mmt

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestTLVReaderSplitsPackets(t *testing.T) {
	first := mmttest.TLV(TLVPacketTypeNull, bytes.Repeat([]byte{0xff}, 10))
	second := mmttest.CompressedIP(1, 2, []byte{0x7f, 0x7f, 0x7f})
	third := mmttest.TLV(TLVPacketTypeTransmissionControl, nil)
	got := readTLVPackets(t, concat(first, second, third))
	if !reflect.DeepEqual(got, [][]byte{first, second, third}) {
		t.Fatalf("packets = %x", got)
	}
}

func TestTLVReaderResynchronizes(t *testing.T) {
	packet := mmttest.CompressedIP(1, 0, []byte{1, 2, 3})
	next := mmttest.CompressedIP(1, 1, []byte{4, 5, 6})
	// The stream starts inside a packet whose data contains the sync byte
	// followed by a plausible header, like a recording cut mid-packet.
	garbage := []byte{0xff, 0x7f, 0x03, 0x00, 0x02, 0xff}
	got := readTLVPackets(t, concat(garbage, packet, next))
	if !reflect.DeepEqual(got, [][]byte{packet, next}) {
		t.Fatalf("packets = %x", got)
	}
}

func TestTLVReaderDropsTrailingPartialPacket(t *testing.T) {
	packet := mmttest.CompressedIP(1, 0, []byte{1, 2, 3})
	truncated := mmttest.CompressedIP(1, 1, []byte{4, 5, 6})
	got := readTLVPackets(t, concat(packet, truncated[:len(truncated)-1]))
	if !reflect.DeepEqual(got, [][]byte{packet}) {
		t.Fatalf("packets = %x", got)
	}
}

func readTLVPackets(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	r := NewTLVReader(bytes.NewReader(stream))
	var packets [][]byte
	for {
		p, err := r.Next()
		if errors.Is(err, io.EOF) {
			return packets
		}
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, append([]byte(nil), p...))
	}
}

func concat(parts ...[]byte) []byte {
	var b []byte
	for _, p := range parts {
		b = append(b, p...)
	}
	return b
}
