package ts

import (
	"bytes"
	"errors"
	"io"
)

const (
	// PacketSize is the size of an ARIB MPEG-2 TS packet in bytes.
	PacketSize              = 188
	packetSizeWithTimestamp = 192
	packetSizeWithParity    = 204
	// SyncByte is the first byte of every TS packet.
	SyncByte = 0x47
)

// Packet represents a single 188-byte MPEG-2 Transport Stream packet.
type Packet []byte

// TransportErrorIndicator returns true if the packet has a transport error.
func (p Packet) TransportErrorIndicator() bool { return p[1]&0x80 != 0 }

// PayloadUnitStartIndicator returns true if this packet starts a PES packet or section.
func (p Packet) PayloadUnitStartIndicator() bool { return p[1]&0x40 != 0 }

// Priority returns the transport priority bit.
func (p Packet) Priority() bool { return p[1]&0x20 != 0 }

// PID returns the 13-bit packet identifier.
func (p Packet) PID() uint16 { return (uint16(p[1]&0x1f) << 8) | uint16(p[2]) }

// HasAdaptationField reports whether the packet contains an adaptation field.
func (p Packet) HasAdaptationField() bool { return (p[3]>>4)&0x03 >= 2 }

// HasPayload reports whether the packet contains a payload.
func (p Packet) HasPayload() bool { return (p[3]>>4)&0x03 == 1 || (p[3]>>4)&0x03 == 3 }

// ContinuityCounter returns the 4-bit continuity counter.
func (p Packet) ContinuityCounter() byte { return p[3] & 0x0f }

// AdaptationFieldLength returns the length of the adaptation field, or 0 if none.
func (p Packet) AdaptationFieldLength() int {
	if len(p) < PacketSize || !p.HasAdaptationField() {
		return 0
	}
	return int(p[4])
}

// PayloadOffset returns the byte offset where the payload begins.
func (p Packet) PayloadOffset() int {
	if !p.HasAdaptationField() {
		return 4
	}
	return 5 + p.AdaptationFieldLength()
}

// ValidPayloadOffset reports whether the packet header points inside the packet.
func (p Packet) ValidPayloadOffset() bool {
	if len(p) < PacketSize {
		return false
	}
	if p.HasAdaptationField() && p.PayloadOffset() > PacketSize {
		return false
	}
	return true
}

// Payload returns the packet payload bytes.
func (p Packet) Payload() []byte {
	if !p.HasPayload() || !p.ValidPayloadOffset() {
		return nil
	}
	return p[p.PayloadOffset():]
}

// IsNull reports whether this is a null packet (PID 0x1fff).
func (p Packet) IsNull() bool { return p.PID() == 0x1fff }

// PacketReader reads TS packets from an io.Reader, recovering sync if necessary.
type PacketReader struct {
	r      io.Reader
	buf    []byte
	stride int
	eof    bool
	tmp    []byte
}

// NewPacketReader creates a new PacketReader.
func NewPacketReader(r io.Reader) *PacketReader {
	return &PacketReader{r: r, buf: make([]byte, 0, 8192), tmp: make([]byte, 4096)}
}

// Next reads the next valid TS packet. It skips garbage until a sync byte is found.
func (pr *PacketReader) Next() (Packet, error) {
	packet := make([]byte, PacketSize)
	return pr.NextInto(packet)
}

// NextInto reads the next valid TS packet into dst. It skips garbage until a
// sync byte is found. dst must be at least PacketSize bytes.
func (pr *PacketReader) NextInto(dst []byte) (Packet, error) {
	if len(dst) < PacketSize {
		return nil, io.ErrShortBuffer
	}
	for {
		if err := pr.fill(minDetectBytes); err != nil && len(pr.buf) == 0 {
			return nil, err
		}
		if len(pr.buf) == 0 {
			return nil, io.EOF
		}

		if pr.stride != 0 {
			if pr.packetReadyAt(0, pr.stride) {
				return pr.consumePacketInto(dst, 0, pr.stride), nil
			}
			pr.stride = 0
		}

		pos, stride, ok := pr.detectPacket()
		if !ok {
			if pr.eof {
				return nil, io.EOF
			}
			if i := bytes.LastIndexByte(pr.buf, SyncByte); i > 0 {
				pr.buf = pr.buf[i:]
			}
			if err := pr.fill(len(pr.buf) + minDetectBytes); err != nil && len(pr.buf) == 0 {
				return nil, err
			}
			continue
		}
		pr.stride = stride
		return pr.consumePacketInto(dst, pos, stride), nil
	}
}

const minDetectBytes = 4 * packetSizeWithParity

func (pr *PacketReader) fill(min int) error {
	for !pr.eof && len(pr.buf) < min {
		n, err := pr.r.Read(pr.tmp)
		if n > 0 {
			pr.buf = append(pr.buf, pr.tmp[:n]...)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				pr.eof = true
				break
			}
			return err
		}
		if n == 0 {
			break
		}
	}
	if len(pr.buf) == 0 && pr.eof {
		return io.EOF
	}
	return nil
}

func (pr *PacketReader) detectPacket() (int, int, bool) {
	strides := [...]int{PacketSize, packetSizeWithTimestamp, packetSizeWithParity}
	for pos := 0; pos < len(pr.buf); pos++ {
		for _, stride := range strides {
			if pr.packetReadyAt(pos, stride) {
				return pos, stride, true
			}
		}
	}
	return 0, 0, false
}

func (pr *PacketReader) packetReadyAt(pos, stride int) bool {
	syncOffset := 0
	if stride == packetSizeWithTimestamp {
		syncOffset = 4
	}
	if pos+syncOffset >= len(pr.buf) || pr.buf[pos+syncOffset] != SyncByte {
		return false
	}
	if len(pr.buf)-pos < stride {
		if !pr.eof {
			_ = pr.fill(pos + stride)
		}
		if len(pr.buf)-pos < stride {
			return false
		}
	}
	checks := 0
	for next := pos + stride + syncOffset; next < len(pr.buf) && checks < 3; next += stride {
		if pr.buf[next] != SyncByte {
			return false
		}
		checks++
	}
	return checks > 0 || pr.eof
}

func (pr *PacketReader) consumePacketInto(dst []byte, pos, stride int) Packet {
	start := pos
	end := pos + stride
	switch stride {
	case packetSizeWithTimestamp:
		start = pos + 4
	case packetSizeWithParity:
		end = pos + PacketSize
	}
	packet := dst[:PacketSize]
	copy(packet, pr.buf[start:end])
	remaining := pr.buf[pos+stride:]
	copy(pr.buf, remaining)
	pr.buf = pr.buf[:len(remaining)]
	return Packet(packet)
}
