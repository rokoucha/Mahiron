// Package mmttest builds TLV packets, MMTP packets, MMT-SI messages and
// sections for tests to assemble streams from, since broadcast recordings
// cannot be committed. It does not import mmt, so tests inside mmt can use it.
package mmttest

import (
	"time"
)

// TLV builds a TLV packet.
func TLV(packetType byte, data []byte) []byte {
	return append([]byte{0x7F, packetType, byte(len(data) >> 8), byte(len(data))}, data...)
}

// CompressedIP builds a TLV packet carrying a header-compressed IP packet
// without the compressed header (header type 0x61), as MMTP packets are sent
// after the first packet of a context.
func CompressedIP(contextID uint16, sequenceNumber byte, payload []byte) []byte {
	data := append([]byte{byte(contextID >> 4), byte(contextID<<4) | sequenceNumber&0x0f, 0x61}, payload...)
	return TLV(0x03, data)
}

// CompressedIPv6UDP builds a TLV packet carrying a header-compressed IP
// packet with the partial IPv6 and UDP headers (header type 0x60), as the
// first packet of a context is sent.
func CompressedIPv6UDP(contextID uint16, sequenceNumber byte, src, dst [16]byte, srcPort, dstPort uint16, payload []byte) []byte {
	data := []byte{byte(contextID >> 4), byte(contextID<<4) | sequenceNumber&0x0f, 0x60}
	data = append(data, 0x60, 0, 0, 0, 17, 255)
	data = append(data, src[:]...)
	data = append(data, dst[:]...)
	data = append(data, byte(srcPort>>8), byte(srcPort), byte(dstPort>>8), byte(dstPort))
	return TLV(0x03, append(data, payload...))
}

// IPv6UDP builds a TLV packet carrying an uncompressed IPv6 UDP datagram,
// as NTP is sent.
func IPv6UDP(src, dst [16]byte, srcPort, dstPort uint16, payload []byte) []byte {
	udpLen := 8 + len(payload)
	b := []byte{0x60, 0, 0, 0, byte(udpLen >> 8), byte(udpLen), 17, 255}
	b = append(b, src[:]...)
	b = append(b, dst[:]...)
	b = append(b, byte(srcPort>>8), byte(srcPort), byte(dstPort>>8), byte(dstPort), byte(udpLen>>8), byte(udpLen), 0, 0)
	return TLV(0x02, append(b, payload...))
}

// MMTP is an MMTP packet to build.
type MMTP struct {
	PayloadType    byte
	PacketID       uint16
	Timestamp      uint32
	SequenceNumber uint32
	RAP            bool
	// PacketCounter is written when non-nil.
	PacketCounter *uint32
	// Extension is written as the header extension area when non-nil.
	ExtensionType uint16
	Extension     []byte
	Payload       []byte
}

// Bytes encodes the packet.
func (p MMTP) Bytes() []byte {
	var flags byte
	if p.PacketCounter != nil {
		flags |= 0x20
	}
	if p.Extension != nil {
		flags |= 0x02
	}
	if p.RAP {
		flags |= 0x01
	}
	b := []byte{flags, p.PayloadType & 0x3f, byte(p.PacketID >> 8), byte(p.PacketID)}
	b = be32(b, p.Timestamp)
	b = be32(b, p.SequenceNumber)
	if p.PacketCounter != nil {
		b = be32(b, *p.PacketCounter)
	}
	if p.Extension != nil {
		b = append(b, byte(p.ExtensionType>>8), byte(p.ExtensionType), byte(len(p.Extension)>>8), byte(len(p.Extension)))
		b = append(b, p.Extension...)
	}
	return append(b, p.Payload...)
}

// SignalingPayload builds a signaling MMTP payload holding one message or
// one fragment of a message.
func SignalingPayload(fragmentationIndicator, fragmentCounter byte, data []byte) []byte {
	return append([]byte{fragmentationIndicator << 6, fragmentCounter}, data...)
}

// AggregatedSignalingPayload builds a signaling MMTP payload aggregating
// messages with 16-bit lengths.
func AggregatedSignalingPayload(messages ...[]byte) []byte {
	b := []byte{0x01, 0}
	for _, m := range messages {
		b = append(b, byte(len(m)>>8), byte(len(m)))
		b = append(b, m...)
	}
	return b
}

// SignalingPackets splits a message into signaling MMTP packets whose
// fragments are at most size bytes, numbered from sequenceNumber.
func SignalingPackets(packetID uint16, sequenceNumber uint32, message []byte, size int) [][]byte {
	if len(message) <= size {
		return [][]byte{MMTP{PayloadType: 0x02, PacketID: packetID, SequenceNumber: sequenceNumber, Payload: SignalingPayload(0, 0, message)}.Bytes()}
	}
	var fragments [][]byte
	for b := message; len(b) > 0; {
		n := min(size, len(b))
		fragments = append(fragments, b[:n])
		b = b[n:]
	}
	packets := make([][]byte, 0, len(fragments))
	for i, f := range fragments {
		indicator := byte(0b10)
		switch i {
		case 0:
			indicator = 0b01
		case len(fragments) - 1:
			indicator = 0b11
		}
		packets = append(packets, MMTP{
			PayloadType:    0x02,
			PacketID:       packetID,
			SequenceNumber: sequenceNumber + uint32(i),
			Payload:        SignalingPayload(indicator, byte(len(fragments)-1-i), f),
		}.Bytes())
	}
	return packets
}

// PAMessage builds a PA message holding tables built by Table.
func PAMessage(version byte, tables ...[]byte) []byte {
	payload := []byte{byte(len(tables))}
	for _, t := range tables {
		payload = append(payload, t[:4]...)
	}
	for _, t := range tables {
		payload = append(payload, t...)
	}
	b := []byte{0x00, 0x00, version}
	b = be32(b, uint32(len(payload)))
	return append(b, payload...)
}

// Table builds an MMT-SI table with a 16-bit length, as carried in a PA
// message.
func Table(tableID, version byte, body []byte) []byte {
	return append([]byte{tableID, version, byte(len(body) >> 8), byte(len(body))}, body...)
}

// M2SectionMessage wraps a section built by Section in an M2 section message.
func M2SectionMessage(version byte, section []byte) []byte {
	return message16(0x8000, version, section)
}

// M2ShortSectionMessage wraps a section built by ShortSection in an M2 short
// section message.
func M2ShortSectionMessage(version byte, section []byte) []byte {
	return message16(0x8002, version, section)
}

func message16(id uint16, version byte, payload []byte) []byte {
	return append([]byte{byte(id >> 8), byte(id), version, byte(len(payload) >> 8), byte(len(payload))}, payload...)
}

// Section builds a long-syntax section with its CRC. body is the part
// after last_section_number.
func Section(tableID byte, tableIDExtension uint16, version, sectionNumber, lastSectionNumber byte, body []byte) []byte {
	length := 5 + len(body) + 4
	b := []byte{tableID, 0xf0 | byte(length>>8)&0x0f, byte(length), byte(tableIDExtension >> 8), byte(tableIDExtension), 0xc1 | (version&0x1f)<<1, sectionNumber, lastSectionNumber}
	return appendCRC(append(b, body...))
}

// ShortSection builds a short-syntax section with its CRC, as MH-TOT is.
func ShortSection(tableID byte, body []byte) []byte {
	length := len(body) + 4
	b := []byte{tableID, 0x70 | byte(length>>8)&0x0f, byte(length)}
	return appendCRC(append(b, body...))
}

// Descriptor builds an MMT-SI descriptor, choosing the width of the length
// field from the tag as ARIB STD-B60 Table 4-10 does.
func Descriptor(tag uint16, data []byte) []byte {
	b := []byte{byte(tag >> 8), byte(tag)}
	switch {
	case tag >= 0x4000 && tag <= 0x6FFF, tag >= 0xF000:
		b = append(b, byte(len(data)>>8), byte(len(data)))
	case tag >= 0x7000 && tag <= 0x7FFF:
		b = be32(b, uint32(len(data)))
	default:
		b = append(b, byte(len(data)))
	}
	return append(b, data...)
}

// TLVDescriptor builds a TLV-SI descriptor, which has an 8-bit tag.
func TLVDescriptor(tag byte, data []byte) []byte {
	return append([]byte{tag, byte(len(data))}, data...)
}

// Loop prefixes descriptors with a 12-bit loop length and 4 reserved bits.
func Loop(descriptors ...[]byte) []byte {
	var b []byte
	for _, d := range descriptors {
		b = append(b, d...)
	}
	return append([]byte{0xf0 | byte(len(b)>>8)&0x0f, byte(len(b))}, b...)
}

// MJDTime encodes t in JST as a 40-bit MJD + BCD time.
func MJDTime(t time.Time) []byte {
	t = t.In(time.FixedZone("JST", 9*60*60))
	epoch := time.Date(1858, time.November, 17, 0, 0, 0, 0, time.UTC)
	date := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	mjd := int(date.Sub(epoch).Hours() / 24)
	return []byte{byte(mjd >> 8), byte(mjd), bcd(t.Hour()), bcd(t.Minute()), bcd(t.Second())}
}

// BCDDuration encodes d as a 24-bit BCD duration.
func BCDDuration(d time.Duration) []byte {
	return []byte{bcd(int(d.Hours())), bcd(int(d.Minutes()) % 60), bcd(int(d.Seconds()) % 60)}
}

func bcd(v int) byte { return byte(v/10<<4 | v%10) }

func be32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendCRC(b []byte) []byte {
	crc := uint32(0xffffffff)
	for _, v := range b {
		crc ^= uint32(v) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return be32(b, crc)
}
