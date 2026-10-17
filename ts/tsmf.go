package ts

import "io"

// TSMFReader extracts a relative TS (1..15) from JCTEA STD-007 §6.3
// multiple frames. Input may contain 188-, 192-, or 204-byte packets.
type TSMFReader struct {
	packets *PacketReader
	target  uint8
	slots   [52]byte
	slot    int
	packet  [PacketSize]byte
	pending []byte
}

func NewTSMFReader(r io.Reader, relativeTS uint8) *TSMFReader {
	return &TSMFReader{packets: NewPacketReader(r), target: relativeTS, slot: 52}
}

func (r *TSMFReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		syncLosses := r.packets.syncLosses
		p, err := r.packets.NextInto(r.packet[:])
		if err != nil {
			return 0, err
		}
		if r.packets.syncLosses != syncLosses {
			r.slot = 52
		}
		if p.PID() == 0x002f {
			// Never use a corrupt or unsupported header's slot assignments.
			r.slot = 52
			sync := uint16(p[4]&0x1f)<<8 | uint16(p[5])
			if p[1] != 0 || p[3]&0xf0 != 0x10 ||
				(sync != 0x1a86 && sync != 0x0579) || p[6]&0x1f != 1 ||
				crc32MPEG2(p[4:]) != 0 {
				continue
			}
			for i := range r.slots {
				r.slots[i] = p[73+i/2] >> (4 * (1 - i%2)) & 0x0f
			}
			r.slot = 0
			continue
		}
		if r.slot >= len(r.slots) {
			continue
		}
		target := r.slots[r.slot]
		r.slot++
		if r.target < 1 || r.target > 15 || target != r.target {
			continue
		}
		r.pending = p
	}
	n := copy(dst, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
