package mmt

import (
	"bytes"
	"testing"

	"github.com/21S1298001/mahiron/mmt/mmttest"
	"github.com/21S1298001/mahiron/packet"
)

func TestDetectorRecognizesTLV(t *testing.T) {
	tlv := append(mmttest.TLV(0xFF, []byte{1, 2, 3}), mmttest.TLV(0xFF, []byte{4})...)
	single := mmttest.TLV(0xFF, []byte{1})

	for _, tc := range []struct {
		name  string
		buf   []byte
		final bool
		want  packet.Result
	}{
		{"two chained packets", tlv, false, packet.Matched},
		{"mid-packet, then two chained packets", append([]byte{0x12, 0x34, 0x00}, tlv...), false, packet.Matched},
		{"single packet at EOF", single, true, packet.Matched},
		{"packet header without its body", single[:2], false, packet.Undecided},
		{"garbage at EOF", []byte{0, 1, 2}, true, packet.Rejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Detector.Detect(tc.buf, tc.final); got != tc.want {
				t.Fatalf("Detect = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDetectorRejectsTS(t *testing.T) {
	tsPacket := append([]byte{0x47}, bytes.Repeat([]byte{0xFF}, 187)...)
	ts := bytes.Repeat(tsPacket, 5)
	if got := Detector.Detect(ts, true); got != packet.Rejected {
		t.Fatalf("Detect(TS, final) = %v, want Rejected", got)
	}
}

func TestDetectorName(t *testing.T) {
	if got := Detector.Name(); got != "tlv" {
		t.Fatalf("Name() = %q, want tlv", got)
	}
}
