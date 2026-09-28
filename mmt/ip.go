package mmt

import (
	"errors"
	"net/netip"
)

// Header types of a header-compressed IP packet (ARIB STD-B32 Part 3, 3.7).
const (
	CompressedHeaderIPv4UDP = 0x20 // partial IPv4 header and partial UDP header
	CompressedHeaderIPv4ID  = 0x21 // IPv4 identification only
	CompressedHeaderIPv6UDP = 0x60 // partial IPv6 header and partial UDP header
	CompressedHeaderNone    = 0x61 // no header
)

const ipProtocolUDP = 17

// ErrNotUDP is returned for TLV packets that do not carry a UDP datagram.
var ErrNotUDP = errors.New("mmt: not a UDP packet")

// UDPPacket is a UDP datagram carried in a TLV packet, either as a plain IP
// packet or as a header-compressed IP packet.
type UDPPacket struct {
	// Compressed reports whether the packet was header-compressed. ContextID
	// and SequenceNumber are set only for compressed packets.
	Compressed     bool
	ContextID      uint16
	SequenceNumber byte
	// Source and Destination are invalid when the compressed packet omits
	// the header (CompressedHeaderIPv4ID and CompressedHeaderNone), which
	// is the usual case after the first packet of a context.
	Source      netip.AddrPort
	Destination netip.AddrPort
	Payload     []byte
}

// ParseUDPPacket extracts the UDP datagram from an IPv4, IPv6 or
// header-compressed IP TLV packet. Payload aliases p.
func ParseUDPPacket(p TLVPacket) (*UDPPacket, error) {
	if len(p) < tlvHeaderSize {
		return nil, ErrInvalidPacket
	}
	data := p.Data()
	switch p.Type() {
	case TLVPacketTypeIPv4:
		return parseIPv4UDP(data)
	case TLVPacketTypeIPv6:
		return parseIPv6UDP(data)
	case TLVPacketTypeCompressedIP:
		return parseCompressedUDP(data)
	default:
		return nil, ErrNotUDP
	}
}

func parseIPv4UDP(b []byte) (*UDPPacket, error) {
	if len(b) < 20 || b[0]>>4 != 4 {
		return nil, ErrInvalidPacket
	}
	headerLen := int(b[0]&0x0f) * 4
	totalLen := int(b[2])<<8 | int(b[3])
	if headerLen < 20 || totalLen < headerLen || totalLen > len(b) {
		return nil, ErrInvalidPacket
	}
	if b[9] != ipProtocolUDP {
		return nil, ErrNotUDP
	}
	src := netip.AddrFrom4([4]byte(b[12:16]))
	dst := netip.AddrFrom4([4]byte(b[16:20]))
	return parseUDP(b[headerLen:totalLen], src, dst)
}

func parseIPv6UDP(b []byte) (*UDPPacket, error) {
	if len(b) < 40 || b[0]>>4 != 6 {
		return nil, ErrInvalidPacket
	}
	payloadLen := int(b[4])<<8 | int(b[5])
	if 40+payloadLen > len(b) {
		return nil, ErrInvalidPacket
	}
	if b[6] != ipProtocolUDP {
		return nil, ErrNotUDP
	}
	src := netip.AddrFrom16([16]byte(b[8:24]))
	dst := netip.AddrFrom16([16]byte(b[24:40]))
	return parseUDP(b[40:40+payloadLen], src, dst)
}

func parseUDP(b []byte, src, dst netip.Addr) (*UDPPacket, error) {
	if len(b) < 8 {
		return nil, ErrInvalidPacket
	}
	length := int(b[4])<<8 | int(b[5])
	if length < 8 || length > len(b) {
		return nil, ErrInvalidPacket
	}
	return &UDPPacket{
		Source:      netip.AddrPortFrom(src, uint16(b[0])<<8|uint16(b[1])),
		Destination: netip.AddrPortFrom(dst, uint16(b[2])<<8|uint16(b[3])),
		Payload:     b[8:length],
	}, nil
}

func parseCompressedUDP(b []byte) (*UDPPacket, error) {
	if len(b) < 3 {
		return nil, ErrInvalidPacket
	}
	u := &UDPPacket{
		Compressed:     true,
		ContextID:      uint16(b[0])<<4 | uint16(b[1]>>4),
		SequenceNumber: b[1] & 0x0f,
	}
	headerType, b := b[2], b[3:]
	switch headerType {
	case CompressedHeaderIPv4UDP:
		// The partial IPv4 header lacks the total length and checksum, and
		// the partial UDP header keeps only the ports.
		if len(b) < 20 || b[0]>>4 != 4 {
			return nil, ErrInvalidPacket
		}
		if b[7] != ipProtocolUDP {
			return nil, ErrNotUDP
		}
		u.Source = netip.AddrPortFrom(netip.AddrFrom4([4]byte(b[8:12])), uint16(b[16])<<8|uint16(b[17]))
		u.Destination = netip.AddrPortFrom(netip.AddrFrom4([4]byte(b[12:16])), uint16(b[18])<<8|uint16(b[19]))
		u.Payload = b[20:]
	case CompressedHeaderIPv4ID:
		if len(b) < 2 {
			return nil, ErrInvalidPacket
		}
		u.Payload = b[2:]
	case CompressedHeaderIPv6UDP:
		// The partial IPv6 header lacks the payload length.
		if len(b) < 42 || b[0]>>4 != 6 {
			return nil, ErrInvalidPacket
		}
		if b[4] != ipProtocolUDP {
			return nil, ErrNotUDP
		}
		u.Source = netip.AddrPortFrom(netip.AddrFrom16([16]byte(b[6:22])), uint16(b[38])<<8|uint16(b[39]))
		u.Destination = netip.AddrPortFrom(netip.AddrFrom16([16]byte(b[22:38])), uint16(b[40])<<8|uint16(b[41]))
		u.Payload = b[42:]
	case CompressedHeaderNone:
		u.Payload = b
	default:
		return nil, ErrInvalidPacket
	}
	return u, nil
}
