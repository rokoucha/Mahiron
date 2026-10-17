package ts

import "io"

// TSMFReader extracts a relative TS (1..15) from JCTEA STD-007 §6.3
// multiple frames. Input may contain 188-, 192-, or 204-byte packets.
type TSMFReader struct {
	packets *PacketReader
	target  uint8
	frame   TSMFFrame
	packet  [PacketSize]byte
	pending []byte
}

func NewTSMFReader(r io.Reader, relativeTS uint8) *TSMFReader {
	return &TSMFReader{packets: NewPacketReader(r), target: relativeTS}
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
			r.frame.Reset()
		}
		relative, ok := r.frame.Feed(p)
		if !ok || relative == 0 || relative != r.target {
			continue
		}
		r.pending = p
	}
	n := copy(dst, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

// TSMFFrame routes packets using the most recent validated frame header.
// Reset must be called when packet synchronization is lost.
type TSMFFrame struct {
	slots  [52]byte
	slot   int
	active bool
}

func (f *TSMFFrame) Reset() { f.active = false }

// Feed returns the relative TS number of a data packet, or zero for a valid
// frame header. Invalid frames and packets outside assigned slots return false.
func (f *TSMFFrame) Feed(p Packet) (uint8, bool) {
	if len(p) != PacketSize {
		f.Reset()
		return 0, false
	}
	if p.PID() == 0x002f {
		f.Reset()
		sync := uint16(p[4]&0x1f)<<8 | uint16(p[5])
		if p[1] != 0 || p[3]&0xf0 != 0x10 || (sync != 0x1a86 && sync != 0x0579) || p[6]&0x1f != 1 || crc32MPEG2(p[4:]) != 0 {
			return 0, false
		}
		for i := range f.slots {
			f.slots[i] = p[73+i/2] >> (4 * (1 - i%2)) & 0x0f
		}
		f.active = true
		f.slot = 0
		return 0, true
	}
	if !f.active || f.slot >= len(f.slots) {
		return 0, false
	}
	relative := f.slots[f.slot]
	f.slot++
	return relative, relative >= 1 && relative <= 15
}
