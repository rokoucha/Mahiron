package mmt

// Message identifiers of MMT-SI (ARIB STD-B60, Table 4-6).
const (
	MessageIDPA               = 0x0000
	MessageIDM2Section        = 0x8000
	MessageIDCA               = 0x8001
	MessageIDM2ShortSection   = 0x8002
	MessageIDDataTransmission = 0x8003
)

// Table identifiers of MMT-SI (ARIB STD-B60, Table 4-8).
const (
	TableIDMPT         = 0x20
	TableIDPLT         = 0x80
	TableIDECMStart    = 0x82
	TableIDECMEnd      = 0x83
	TableIDMHEITPF     = 0x8B // present/following, self-stream
	TableIDMHEITSStart = 0x8C // schedule, self-stream
	TableIDMHEITSEnd   = 0x9B // schedule, self-stream
	TableIDMHSDTActual = 0x9F
	TableIDMHSDTOther  = 0xA0
	TableIDMHTOT       = 0xA1
	TableIDMHCDT       = 0xA2
)

// Packet IDs of MMTP packets carrying MMT-SI (ARIB STD-B60, Table 4-11).
const (
	PacketIDPA    = 0x0000
	PacketIDCA    = 0x0001
	PacketIDMHEIT = 0x8000
	PacketIDMHAIT = 0x8001
	PacketIDMHBIT = 0x8002
	PacketIDMHSDT = 0x8004
	PacketIDMHTOT = 0x8005
	PacketIDMHCDT = 0x8006
)

// Message is a whole MMT-SI message including its header.
type Message []byte

// ID returns the message_id. The message must be at least 2 bytes long.
func (m Message) ID() uint16 { return uint16(m[0])<<8 | uint16(m[1]) }

// hasLength32 reports whether the message's length field is 32 bits wide.
// The PA and MPI messages and the 0x7000–0x7FFF and 0xF000–0xFFFF ranges use
// 32 bits; the others use 16 bits.
func (m Message) hasLength32() bool {
	id := m.ID()
	return id <= 0x000F || (id >= 0x7000 && id <= 0x7FFF) || id >= 0xF000
}

// Payload returns the version and the bytes following the length field.
func (m Message) Payload() (version byte, payload []byte, err error) {
	if len(m) < 5 {
		return 0, nil, ErrInvalidSection
	}
	var length, off int
	if m.hasLength32() {
		if len(m) < 7 {
			return 0, nil, ErrInvalidSection
		}
		length, off = int(be32(m[3:])), 7
	} else {
		length, off = int(m[3])<<8|int(m[4]), 5
	}
	if length > len(m)-off {
		return 0, nil, ErrInvalidSection
	}
	return m[2], m[off : off+length], nil
}

// Section returns the section carried by an M2 section message or an M2
// short section message. It aliases m.
func (m Message) Section() (Section, error) {
	if len(m) < 2 || (m.ID() != MessageIDM2Section && m.ID() != MessageIDM2ShortSection) {
		return nil, ErrInvalidSection
	}
	_, payload, err := m.Payload()
	if err != nil {
		return nil, err
	}
	s := Section(payload)
	if len(s) < 3 || s.TotalLength() > len(s) {
		return nil, ErrInvalidSection
	}
	return s[:s.TotalLength()], nil
}

// Table is an MMT-SI table carried in a PA message: table_id, version, a
// 16-bit length and the table body.
type Table []byte

// TableID returns the table_id.
func (t Table) TableID() byte { return t[0] }

// Version returns the table version.
func (t Table) Version() byte { return t[1] }

// Data returns the table body following the length field.
func (t Table) Data() []byte { return t[4:] }

// body returns the table body after checking the table_id and the length.
func (t Table) body(isTableID func(byte) bool) ([]byte, error) {
	if len(t) < 4 || !isTableID(t.TableID()) || 4+(int(t[2])<<8|int(t[3])) > len(t) {
		return nil, ErrInvalidSection
	}
	return t[4 : 4+(int(t[2])<<8|int(t[3]))], nil
}

// PAMessage is a Package Access message (ARIB STD-B60, 7.2.3.1).
type PAMessage struct {
	Version byte
	Tables  []Table
}

// ParsePAMessage parses a PA message. Tables alias m.
func ParsePAMessage(m Message) (*PAMessage, error) {
	if len(m) < 2 || m.ID() != MessageIDPA {
		return nil, ErrInvalidSection
	}
	version, payload, err := m.Payload()
	if err != nil {
		return nil, err
	}
	if len(payload) < 1 {
		return nil, ErrInvalidSection
	}
	// The table headers in the extension repeat what each table carries
	// itself, so the tables are split by their own length fields.
	off := 1 + int(payload[0])*4
	if off > len(payload) {
		return nil, ErrInvalidSection
	}
	pa := &PAMessage{Version: version}
	for b := payload[off:]; len(b) > 0; {
		if len(b) < 4 {
			return nil, ErrInvalidSection
		}
		size := 4 + (int(b[2])<<8 | int(b[3]))
		if size > len(b) {
			return nil, ErrInvalidSection
		}
		pa.Tables = append(pa.Tables, Table(b[:size]))
		b = b[size:]
	}
	return pa, nil
}
