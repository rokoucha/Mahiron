package ts

// PSI/SI service-discovery scenarios follow ARIB STD-B10 and the terrestrial
// and satellite operational constraints in TR-B14/TR-B15.

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseSDTParsesServiceDescriptors(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{
		{
			serviceID: 100,
			descriptors: serviceDescriptor(1, nil, []byte{
				0x0e, 'N', 'H', 'K', 0x0f, 0x41, 0x6d,
			}),
		},
	})

	sdt, err := ParseSDT(section)
	if err != nil {
		t.Fatal(err)
	}
	if sdt.TransportStreamID != 0x1234 || sdt.OriginalNetworkID != 0x5678 {
		t.Fatalf("SDT ids = %#v/%#v, want 0x1234/0x5678", sdt.TransportStreamID, sdt.OriginalNetworkID)
	}
	if len(sdt.Services) != 1 || sdt.Services[0].ServiceID != 100 {
		t.Fatalf("SDT services = %#v", sdt.Services)
	}
	desc, err := ParseServiceDescriptor(sdt.Services[0].Descriptors[0])
	if err != nil {
		t.Fatal(err)
	}
	if desc.ServiceType != 1 || desc.ServiceName != "ＮＨＫ総" {
		t.Fatalf("service descriptor = %#v", desc)
	}
}

func TestParseSDTRejectsBrokenCRC(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, nil)
	section[len(section)-1] ^= 0xff
	if _, err := ParseSDT(section); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("ParseSDT error = %v, want ErrInvalidSection", err)
	}
}

func TestTableSectionSetResetsOnVersionChange(t *testing.T) {
	var sections tableSectionSet
	v0s0 := withTableHeader(buildNIT(t), TableIDNIT0, 0, 0, 1)
	v1s1 := withTableHeader(buildNIT(t), TableIDNIT0, 1, 1, 1)
	v1s0 := withTableHeader(buildNIT(t), TableIDNIT0, 1, 0, 1)

	if _, ready := sections.add(v0s0); ready {
		t.Fatal("table became ready with only version 0 section 0")
	}
	if reset, ready := sections.add(v1s1); !reset || ready {
		t.Fatalf("version change = reset %v, ready %v; want true, false", reset, ready)
	}
	if _, ready := sections.add(v1s0); !ready {
		t.Fatal("version 1 table did not become ready with both version 1 sections")
	}
}

func TestParseTSInformationDescriptor(t *testing.T) {
	desc := tsInformationDescriptor(7, aribAlnum("TOKYO"), []tsInformationTransmissionSpec{
		{
			info:       0x81,
			serviceIDs: []uint16{100, 101},
		},
		{
			info:       0x82,
			serviceIDs: []uint16{200},
		},
	})

	got, err := ParseTSInformationDescriptor(desc)
	if err != nil {
		t.Fatal(err)
	}
	want := &TSInformationDescriptor{
		RemoteControlKeyID: 7,
		TSName:             "ＴＯＫＹＯ",
		TransmissionTypes: []TSInformationTransmissionType{
			{TransmissionTypeInfo: 0x81, ServiceIDs: []uint16{100, 101}},
			{TransmissionTypeInfo: 0x82, ServiceIDs: []uint16{200}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TS information descriptor = %#v, want %#v", got, want)
	}
}

func TestParseTSInformationDescriptorRejectsInvalidLengths(t *testing.T) {
	for _, desc := range []Descriptor{
		descriptor(DescriptorTagTSInformation, []byte{7}),
		descriptor(DescriptorTagTSInformation, []byte{7, 4 << 2, 'A'}),
		descriptor(DescriptorTagTSInformation, []byte{7, 0x01}),
		descriptor(DescriptorTagTSInformation, []byte{7, 0x01, 0x80, 2, 0, 100}),
	} {
		if _, err := ParseTSInformationDescriptor(desc); !errors.Is(err, ErrInvalidSection) {
			t.Fatalf("ParseTSInformationDescriptor(%#v) error = %v, want ErrInvalidSection", desc, err)
		}
	}
}

type sdtServiceSpec struct {
	serviceID   uint16
	descriptors []byte
}

type tsInformationTransmissionSpec struct {
	info       byte
	serviceIDs []uint16
}

func tsInformationDescriptor(remoteControlKeyID uint8, name []byte, transmissions []tsInformationTransmissionSpec) Descriptor {
	data := []byte{remoteControlKeyID, byte(len(name)<<2) | byte(len(transmissions)&0x03)}
	data = append(data, name...)
	for _, transmission := range transmissions {
		data = append(data, transmission.info, byte(len(transmission.serviceIDs)))
		for _, serviceID := range transmission.serviceIDs {
			data = append(data, byte(serviceID>>8), byte(serviceID))
		}
	}
	return descriptor(DescriptorTagTSInformation, data)
}

func buildSDT(t *testing.T, tsid, onid uint16, services []sdtServiceSpec) Section {
	t.Helper()
	serviceLoopLen := 0
	for _, svc := range services {
		serviceLoopLen += 5 + len(svc.descriptors)
	}
	sectionLength := 8 + serviceLoopLen + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDSDT0
	s[1] = 0xf0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3] = byte(tsid >> 8)
	s[4] = byte(tsid)
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8] = byte(onid >> 8)
	s[9] = byte(onid)
	s[10] = 0xff
	off := 11
	for _, svc := range services {
		s[off] = byte(svc.serviceID >> 8)
		s[off+1] = byte(svc.serviceID)
		s[off+2] = 0xff
		s[off+3] = 0xf0 | byte(len(svc.descriptors)>>8)
		s[off+4] = byte(len(svc.descriptors))
		copy(s[off+5:], svc.descriptors)
		off += 5 + len(svc.descriptors)
	}
	writeCRC(s)
	return Section(s)
}

func buildNIT(t *testing.T) Section {
	t.Helper()
	return buildNITWithTransportStreams(t, nil)
}

type nitTransportSpec struct {
	tsid        uint16
	onid        uint16
	descriptors []byte
}

func buildNITWithTransportStreams(t *testing.T, transports []nitTransportSpec) Section {
	t.Helper()
	tsLoopLen := 0
	for _, transport := range transports {
		tsLoopLen += 6 + len(transport.descriptors)
	}
	sectionLength := 13 + tsLoopLen
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDNIT0
	s[1] = 0xf0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = 0x56, 0x78
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8], s[9] = 0xf0, 0
	s[10], s[11] = 0xf0|byte(tsLoopLen>>8), byte(tsLoopLen)
	off := 12
	for _, transport := range transports {
		s[off] = byte(transport.tsid >> 8)
		s[off+1] = byte(transport.tsid)
		s[off+2] = byte(transport.onid >> 8)
		s[off+3] = byte(transport.onid)
		s[off+4] = 0xf0 | byte(len(transport.descriptors)>>8)
		s[off+5] = byte(len(transport.descriptors))
		copy(s[off+6:], transport.descriptors)
		off += 6 + len(transport.descriptors)
	}
	writeCRC(s)
	return Section(s)
}

func withTableHeader(section Section, tableID, version, number, last byte) Section {
	section = append(Section(nil), section...)
	section[0] = tableID
	section[5] = 0xc1 | ((version & 0x1f) << 1)
	section[6] = number
	section[7] = last
	writeCRC(section)
	return section
}

func serviceDescriptor(serviceType uint8, providerName, serviceName []byte) []byte {
	data := []byte{serviceType, byte(len(providerName))}
	data = append(data, providerName...)
	data = append(data, byte(len(serviceName)))
	data = append(data, serviceName...)
	return append([]byte{DescriptorTagService, byte(len(data))}, data...)
}
