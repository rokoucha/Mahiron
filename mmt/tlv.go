// Package mmt parses the control information of ISDB-S3 streams: TLV
// packets (ARIB STD-B32 Part 3), MMTP packets and MMT-SI messages, tables and
// descriptors (ARIB STD-B60). Video and audio payloads are left untouched.
package mmt

import (
	"bufio"
	"errors"
	"io"
)

// TLV packet types (ARIB STD-B32 Part 3, 3.5).
const (
	TLVPacketTypeIPv4                = 0x01
	TLVPacketTypeIPv6                = 0x02
	TLVPacketTypeCompressedIP        = 0x03
	TLVPacketTypeTransmissionControl = 0xFE
	TLVPacketTypeNull                = 0xFF
)

// TLVSyncByte is the first byte of every TLV packet: '01' followed by six
// reserved '1' bits.
const TLVSyncByte = 0x7F

const tlvHeaderSize = 4

var ErrInvalidPacket = errors.New("mmt: invalid packet")

// TLVPacket is a whole TLV packet including its 4-byte header.
type TLVPacket []byte

// Type returns the packet_type.
func (p TLVPacket) Type() byte { return p[1] }

// Data returns the bytes following the header.
func (p TLVPacket) Data() []byte { return p[tlvHeaderSize:] }

// TLVReader splits a byte stream into TLV packets. Because TLV packets have
// no fixed size, it only trusts a header after resynchronizing when the next
// packet also starts with the sync byte.
type TLVReader struct {
	r      *bufio.Reader
	synced bool
}

// NewTLVReader returns a reader that splits r into TLV packets.
func NewTLVReader(r io.Reader) *TLVReader {
	// The buffer must hold the largest packet plus the next header.
	return &TLVReader{r: bufio.NewReaderSize(r, tlvHeaderSize+0xFFFF+1)}
}

// Next returns the next TLV packet. The returned slice is only valid until
// the next call. It returns io.EOF at the end of the stream, dropping any
// trailing partial packet.
func (r *TLVReader) Next() (TLVPacket, error) {
	for {
		header, err := r.r.Peek(tlvHeaderSize)
		if err != nil {
			return nil, err
		}
		if header[0] != TLVSyncByte {
			r.synced = false
			if _, err := r.r.Discard(1); err != nil {
				return nil, err
			}
			continue
		}
		size := tlvHeaderSize + (int(header[2])<<8 | int(header[3]))
		if !r.synced {
			next, err := r.r.Peek(size + 1)
			if err == nil && next[size] != TLVSyncByte {
				if _, err := r.r.Discard(1); err != nil {
					return nil, err
				}
				continue
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}
		}
		packet, err := r.r.Peek(size)
		if err != nil {
			return nil, err
		}
		if _, err := r.r.Discard(size); err != nil {
			return nil, err
		}
		r.synced = true
		return TLVPacket(packet), nil
	}
}
