package ts

import "github.com/21S1298001/mahiron/packet"

// Detector recognizes plain MPEG-2 TS at the start of a byte stream (see the
// packet package), before handing the bytes to the more lenient PacketReader.
var Detector packet.Detector = formatDetector{}

type formatDetector struct{}

func (formatDetector) Name() string { return "ts" }

// tsStrides lists the packet strides PacketReader resyncs on: plain
// 188-byte packets, packets preceded by a 4-byte capture timestamp, and
// packets followed by 16 bytes of FEC parity.
var tsStrides = [...]struct{ stride, syncOffset int }{
	{PacketSize, 0},
	{packetSizeWithTimestamp, 4},
	{packetSizeWithParity, 0},
}

func (formatDetector) Detect(buf []byte, final bool) packet.Result {
	for _, s := range tsStrides {
		if hasSyncRun(buf, s.stride, s.syncOffset, 5) {
			return packet.Matched
		}
	}
	if final && len(buf) > 0 && buf[0] == SyncByte {
		// A tiny finite source, such as a test fixture, can end after a
		// single packet, too short for the run check above.
		return packet.Matched
	}
	if final {
		return packet.Rejected
	}
	return packet.Undecided
}

// hasSyncRun reports whether buf holds want sync bytes in a row, stride
// apart, starting within the first packet's worth of buf.
func hasSyncRun(buf []byte, stride, syncOffset, want int) bool {
	scanWindow := min(len(buf), PacketSize)
	for i := 0; i < scanWindow; i++ {
		if i+syncOffset >= len(buf) || buf[i+syncOffset] != SyncByte {
			continue
		}
		matched := 1
		for next := i + stride; next+syncOffset < len(buf) && matched < want; next += stride {
			if buf[next+syncOffset] != SyncByte {
				break
			}
			matched++
		}
		if matched >= want {
			return true
		}
	}
	return false
}
