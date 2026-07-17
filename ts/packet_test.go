package ts

// Transport packet, PAT and PMT vectors cover the MPEG-TS framing used by the
// current ARIB STD-B10/TR-B14/TR-B15 broadcast profiles.

import (
	"bytes"
	"testing"
)

func TestPacketReaderNormalizesPacketSizes(t *testing.T) {
	packet := payloadPacket(0x0100, []byte{1, 2, 3}, 0)
	for _, tc := range []struct {
		name   string
		input  []byte
		prefix []byte
	}{
		{name: "188", input: packet},
		{name: "192", input: append(append([]byte{0, 1, 2, 3}, packet...), 0, 1, 2, 3)},
		{name: "204", input: append(append([]byte{}, packet...), bytes.Repeat([]byte{0xee}, 16)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := NewPacketReader(bytes.NewReader(tc.input))
			got, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != PacketSize {
				t.Fatalf("packet length = %d, want %d", len(got), PacketSize)
			}
			if !bytes.Equal(got, packet) {
				t.Fatalf("packet mismatch")
			}
		})
	}
}

func TestPacketProgramClockReference(t *testing.T) {
	p := make(Packet, PacketSize)
	p[0], p[1], p[2], p[3], p[4], p[5] = SyncByte, 0x01, 0x00, 0x20, 7, 0x10
	base, extension := uint64(0x123456789), uint16(0x155)
	p[6], p[7], p[8], p[9] = byte(base>>25), byte(base>>17), byte(base>>9), byte(base>>1)
	p[10], p[11] = byte(base&1)<<7|0x7e|byte(extension>>8), byte(extension)
	gotBase, gotExtension, ok := p.ProgramClockReference()
	if !ok || gotBase != base || gotExtension != extension {
		t.Fatalf("PCR = %#x/%#x/%v, want %#x/%#x", gotBase, gotExtension, ok, base, extension)
	}
}

func TestPacketDiscontinuityIndicator(t *testing.T) {
	packet := payloadPacket(0x0100, []byte{1}, 0)
	if packet.DiscontinuityIndicator() {
		t.Fatal("payload-only packet has discontinuity indicator")
	}
	packet[3] = 0x30
	packet[4] = 1
	packet[5] = 0x80
	if !packet.DiscontinuityIndicator() {
		t.Fatal("adaptation field discontinuity indicator was not detected")
	}
	packet[4] = 0
	if packet.DiscontinuityIndicator() {
		t.Fatal("zero-length adaptation field has discontinuity indicator")
	}
}

func TestPacketReaderResyncsAfterGarbage(t *testing.T) {
	packet := payloadPacket(0x0100, []byte{1}, 0)
	input := append([]byte{0, 1, 2, 3, 4}, packet...)
	reader := NewPacketReader(bytes.NewReader(input))
	got, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got.PID() != 0x0100 {
		t.Fatalf("PID = %#04x, want 0x0100", got.PID())
	}
}

func collectSections(t *testing.T, packets []Packet, pid uint16) []Section {
	t.Helper()
	assembler := NewSectionAssembler(pid)
	var sections []Section
	for _, p := range packets {
		if p.PID() != pid {
			continue
		}
		got, err := assembler.FeedAll(p)
		if err != nil {
			t.Fatal(err)
		}
		sections = append(sections, got...)
	}
	return sections
}

func buildPAT(t *testing.T, programs map[uint16]uint16) Section {
	t.Helper()
	sectionLength := 5 + len(programs)*4 + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDPAT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = 0x12, 0x34
	s[5], s[6], s[7] = 0xc1, 0, 0
	off := 8
	for serviceID, pmtPID := range programs {
		s[off] = byte(serviceID >> 8)
		s[off+1] = byte(serviceID)
		s[off+2] = 0xe0 | byte(pmtPID>>8)
		s[off+3] = byte(pmtPID)
		off += 4
	}
	writeCRC(s)
	return Section(s)
}

func buildCAT(t *testing.T, emmPID uint16) Section {
	t.Helper()
	descriptors := caDescriptor(emmPID)
	sectionLength := 5 + len(descriptors) + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDCAT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[5], s[6], s[7] = 0xc1, 0, 0
	copy(s[8:], descriptors)
	writeCRC(s)
	return Section(s)
}

func buildPMT(t *testing.T, serviceID, pcrPID uint16, esPIDs []uint16, caPIDs []uint16) Section {
	t.Helper()
	var descriptors []byte
	for _, pid := range caPIDs {
		descriptors = append(descriptors, caDescriptor(pid)...)
	}
	bodyLen := 9 + len(descriptors) + len(esPIDs)*5 + 4
	s := make([]byte, 3+bodyLen)
	s[0] = TableIDPMT
	s[1] = 0xb0 | byte(bodyLen>>8)
	s[2] = byte(bodyLen)
	s[3] = byte(serviceID >> 8)
	s[4] = byte(serviceID)
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8] = 0xe0 | byte(pcrPID>>8)
	s[9] = byte(pcrPID)
	s[10] = 0xf0 | byte(len(descriptors)>>8)
	s[11] = byte(len(descriptors))
	copy(s[12:], descriptors)
	off := 12 + len(descriptors)
	for _, pid := range esPIDs {
		s[off] = 0x1b
		s[off+1] = 0xe0 | byte(pid>>8)
		s[off+2] = byte(pid)
		s[off+3] = 0xf0
		s[off+4] = 0
		off += 5
	}
	writeCRC(s)
	return Section(s)
}

func caDescriptor(pid uint16) []byte {
	return []byte{DescriptorTagCA, 4, 0, 5, 0xe0 | byte(pid>>8), byte(pid)}
}

func writeCRC(s []byte) {
	crc := crc32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
}

func sectionPackets(pid uint16, section Section, counter byte) []byte {
	packets := packetizeSection(pid, section, &counter)
	var out []byte
	for _, p := range packets {
		out = append(out, p...)
	}
	return out
}

func payloadPacket(pid uint16, payload []byte, counter byte) Packet {
	packet := make([]byte, PacketSize)
	for i := range packet {
		packet[i] = 0xff
	}
	packet[0] = SyncByte
	packet[1] = byte(pid >> 8)
	packet[2] = byte(pid)
	packet[3] = 0x10 | (counter & 0x0f)
	copy(packet[4:], payload)
	return Packet(packet)
}
