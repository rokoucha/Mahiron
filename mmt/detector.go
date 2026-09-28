package mmt

import "github.com/21S1298001/mahiron/packet"

// Detector recognizes ISDB-S3 TLV at the start of a byte stream (see the
// packet package), before handing the bytes to TLVReader.
var Detector packet.Detector = formatDetector{}

type formatDetector struct{}

func (formatDetector) Name() string { return "tlv" }

// Detect looks for two TLV packets chained by their length field, or one
// that ends exactly where a finished stream ends.
func (formatDetector) Detect(buf []byte, final bool) packet.Result {
	for i := 0; i+4 <= len(buf); i++ {
		if buf[i] != TLVSyncByte {
			continue
		}
		next := i + 4 + (int(buf[i+2])<<8 | int(buf[i+3]))
		switch {
		case next < len(buf) && buf[next] == TLVSyncByte:
			return packet.Matched
		case next == len(buf) && final:
			return packet.Matched
		}
	}
	if final {
		return packet.Rejected
	}
	return packet.Undecided
}
