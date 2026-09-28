package mmt

// MMTP payload types (ARIB STD-B60, Table 6-6).
const (
	MMTPPayloadTypeMPU       = 0x00
	MMTPPayloadTypeSignaling = 0x02
)

// Fragmentation indicator values of MMTP payloads (ARIB STD-B60, Table 6-3).
const (
	FragmentComplete = 0b00
	FragmentFirst    = 0b01
	FragmentMiddle   = 0b10
	FragmentLast     = 0b11
)

// MMTPPacket is a parsed MMTP packet header (ARIB STD-B60, 6.4.1.1).
type MMTPPacket struct {
	Version              byte
	FECType              byte
	RAP                  bool
	PayloadType          byte
	PacketID             uint16
	Timestamp            uint32
	PacketSequenceNumber uint32
	HasPacketCounter     bool
	PacketCounter        uint32
	// Extension is nil when the packet has no header extension.
	Extension *MMTPHeaderExtension
	Payload   []byte
}

// MMTPHeaderExtension is the header extension area of an MMTP packet.
type MMTPHeaderExtension struct {
	Type uint16
	Data []byte
}

// ParseMMTPPacket parses an MMTP packet, usually the payload of a UDP
// datagram. Payload and Extension.Data alias b.
func ParseMMTPPacket(b []byte) (*MMTPPacket, error) {
	if len(b) < 12 {
		return nil, ErrInvalidPacket
	}
	p := &MMTPPacket{
		Version:              b[0] >> 6,
		HasPacketCounter:     b[0]&0x20 != 0,
		FECType:              (b[0] >> 3) & 0x03,
		RAP:                  b[0]&0x01 != 0,
		PayloadType:          b[1] & 0x3f,
		PacketID:             uint16(b[2])<<8 | uint16(b[3]),
		Timestamp:            be32(b[4:]),
		PacketSequenceNumber: be32(b[8:]),
	}
	off := 12
	if p.HasPacketCounter {
		if len(b) < off+4 {
			return nil, ErrInvalidPacket
		}
		p.PacketCounter = be32(b[off:])
		off += 4
	}
	if b[0]&0x02 != 0 {
		if len(b) < off+4 {
			return nil, ErrInvalidPacket
		}
		length := int(b[off+2])<<8 | int(b[off+3])
		if len(b) < off+4+length {
			return nil, ErrInvalidPacket
		}
		p.Extension = &MMTPHeaderExtension{
			Type: uint16(b[off])<<8 | uint16(b[off+1]),
			Data: b[off+4 : off+4+length],
		}
		off += 4 + length
	}
	p.Payload = b[off:]
	return p, nil
}

// SignalingPayload is the MMTP payload of a signaling message packet
// (ARIB STD-B60, Table 6-1, payload_type 0x02).
type SignalingPayload struct {
	FragmentationIndicator byte
	LengthExtension        bool
	Aggregation            bool
	FragmentCounter        byte
	Data                   []byte
}

// ParseSignalingPayload parses the payload of a signaling MMTP packet. Data
// aliases b.
func ParseSignalingPayload(b []byte) (*SignalingPayload, error) {
	if len(b) < 2 {
		return nil, ErrInvalidPacket
	}
	return &SignalingPayload{
		FragmentationIndicator: b[0] >> 6,
		LengthExtension:        b[0]&0x02 != 0,
		Aggregation:            b[0]&0x01 != 0,
		FragmentCounter:        b[1],
		Data:                   b[2:],
	}, nil
}

// Messages splits the payload into complete messages. It returns nil for a
// fragment of a divided message; use MessageAssembler to join those.
func (s *SignalingPayload) Messages() ([]Message, error) {
	if s.FragmentationIndicator != FragmentComplete {
		return nil, nil
	}
	if !s.Aggregation {
		return []Message{Message(s.Data)}, nil
	}
	var messages []Message
	for b := s.Data; len(b) > 0; {
		var length int
		if s.LengthExtension {
			if len(b) < 4 {
				return nil, ErrInvalidPacket
			}
			length, b = int(be32(b)), b[4:]
		} else {
			if len(b) < 2 {
				return nil, ErrInvalidPacket
			}
			length, b = int(b[0])<<8|int(b[1]), b[2:]
		}
		if length > len(b) {
			return nil, ErrInvalidPacket
		}
		messages = append(messages, Message(b[:length]))
		b = b[length:]
	}
	return messages, nil
}

// maxMessageSize bounds a message joined from fragments so a broken stream
// cannot grow the buffer without limit; the largest in practice, MH-EIT, is
// at most 4 KiB.
const maxMessageSize = 1 << 20

// MessageAssembler joins signaling messages divided across MMTP packets,
// tracked per IP data flow and packet_id, dropping a message with a missing
// fragment or beyond maxMessageSize.
type MessageAssembler struct {
	pending map[PacketKey]*pendingMessage
}

type pendingMessage struct {
	data           []byte
	sequenceNumber uint32
}

// Feed feeds a signaling MMTP packet received on flow and returns the
// messages it completes. Returned messages do not alias the packet.
func (a *MessageAssembler) Feed(flow IPDataFlow, p *MMTPPacket) ([]Message, error) {
	if p.PayloadType != MMTPPayloadTypeSignaling {
		return nil, nil
	}
	s, err := ParseSignalingPayload(p.Payload)
	if err != nil {
		return nil, err
	}
	key := PacketKey{Flow: flow, PacketID: p.PacketID}
	pending := a.pending[key]
	switch s.FragmentationIndicator {
	case FragmentComplete:
		delete(a.pending, key)
		messages, err := s.Messages()
		if err != nil {
			return nil, err
		}
		for i, m := range messages {
			messages[i] = append(Message(nil), m...)
		}
		return messages, nil
	case FragmentFirst:
		if a.pending == nil {
			a.pending = make(map[PacketKey]*pendingMessage)
		}
		a.pending[key] = &pendingMessage{
			data:           append([]byte(nil), s.Data...),
			sequenceNumber: p.PacketSequenceNumber,
		}
		return nil, nil
	}
	// Middle or last fragment: it must directly follow the previous one.
	if pending == nil || p.PacketSequenceNumber != pending.sequenceNumber+1 || len(pending.data)+len(s.Data) > maxMessageSize {
		delete(a.pending, key)
		return nil, nil
	}
	pending.data = append(pending.data, s.Data...)
	pending.sequenceNumber = p.PacketSequenceNumber
	if s.FragmentationIndicator == FragmentMiddle {
		return nil, nil
	}
	delete(a.pending, key)
	joined := *s
	joined.FragmentationIndicator = FragmentComplete
	joined.Data = pending.data
	return joined.Messages()
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
