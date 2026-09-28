package ts

import (
	"bytes"
	"testing"

	"github.com/21S1298001/mahiron/packet"
)

func TestDetectorRecognizesTS(t *testing.T) {
	plain := bytes.Repeat(append([]byte{SyncByte}, bytes.Repeat([]byte{0xFF}, PacketSize-1)...), 5)
	timestamped := bytes.Repeat(append([]byte{0, 1, 2, 3, SyncByte}, bytes.Repeat([]byte{0xFF}, PacketSize-1)...), 5)
	withParity := bytes.Repeat(append(append([]byte{SyncByte}, bytes.Repeat([]byte{0xFF}, PacketSize-1)...), bytes.Repeat([]byte{0xEE}, 16)...), 5)
	single := append([]byte{SyncByte}, bytes.Repeat([]byte{0xFF}, PacketSize-1)...)

	for _, tc := range []struct {
		name  string
		buf   []byte
		final bool
		want  packet.Result
	}{
		{"plain 188-byte packets", plain, false, packet.Matched},
		{"packets with a capture timestamp", timestamped, false, packet.Matched},
		{"packets with FEC parity", withParity, false, packet.Matched},
		{"single packet at EOF", single, true, packet.Matched},
		{"header without enough packets, more data coming", single, false, packet.Undecided},
		{"garbage at EOF", []byte{0, 1, 2}, true, packet.Rejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Detector.Detect(tc.buf, tc.final); got != tc.want {
				t.Fatalf("Detect = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDetectorRejectsTLV(t *testing.T) {
	// Two chained TLV packets: sync byte, packet type, then a 2-byte
	// length that points at the next sync byte.
	tlv := append([]byte{0x7F, 0xFF, 0x00, 0x03, 1, 2, 3}, []byte{0x7F, 0xFF, 0x00, 0x01, 9}...)
	if got := Detector.Detect(tlv, true); got != packet.Rejected {
		t.Fatalf("Detect(TLV, final) = %v, want Rejected", got)
	}
}

func TestDetectorName(t *testing.T) {
	if got := Detector.Name(); got != "ts" {
		t.Fatalf("Name() = %q, want ts", got)
	}
}
