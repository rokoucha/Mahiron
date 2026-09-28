package mmt

// Table identifiers of TLV-SI (ARIB STD-B60, Table 4-2). TLV-SI sections
// are carried directly in TLV transmission control signal packets.
const (
	TableIDTLVNITActual = 0x40
	TableIDTLVNITOther  = 0x41
	TableIDAMT          = 0xFE
)

// Descriptor tags of TLV-SI (ARIB STD-B60, Table 4-4).
const (
	TLVDescriptorTagNetworkName             = 0x40
	TLVDescriptorTagServiceList             = 0x41
	TLVDescriptorTagSatelliteDeliverySystem = 0x43
	TLVDescriptorTagRemoteControlKey        = 0xCD
	TLVDescriptorTagSystemManagement        = 0xFE
)

// TLVDescriptor is a TLV-SI descriptor, which has an 8-bit tag and length
// unlike MMT-SI descriptors.
type TLVDescriptor struct {
	Tag  byte
	Data []byte
}

func parseTLVDescriptors(b []byte) ([]TLVDescriptor, error) {
	var descriptors []TLVDescriptor
	for len(b) > 0 {
		if len(b) < 2 || int(b[1]) > len(b)-2 {
			return nil, ErrInvalidSection
		}
		descriptors = append(descriptors, TLVDescriptor{Tag: b[0], Data: b[2 : 2+int(b[1])]})
		b = b[2+int(b[1]):]
	}
	return descriptors, nil
}

// TLVNIT is a Network Information Table for TLV section (ARIB STD-B60,
// 5.2.1.1).
type TLVNIT struct {
	SectionHeader
	NetworkID          uint16
	NetworkDescriptors []TLVDescriptor
	TLVStreams         []TLVNITStream
}

type TLVNITStream struct {
	TLVStreamID       uint16
	OriginalNetworkID uint16
	Descriptors       []TLVDescriptor
}

// ParseTLVNIT parses a TLV-NIT section, usually the data of a TLV
// transmission control signal packet. Descriptors alias s.
func ParseTLVNIT(s Section) (*TLVNIT, error) {
	header, body, err := parseLongSection(s, 4, TableIDTLVNITActual, TableIDTLVNITOther)
	if err != nil {
		return nil, err
	}
	r := newReader(body)
	networkDescriptors, err := parseTLVDescriptors(r.bytes(int(r.u16() & 0x0fff)))
	if err != nil {
		return nil, err
	}
	nit := &TLVNIT{SectionHeader: header, NetworkID: header.TableIDExtension, NetworkDescriptors: networkDescriptors}
	loop := newReader(r.bytes(int(r.u16() & 0x0fff)))
	if err := r.err(); err != nil || len(r.b) != 0 {
		return nil, ErrInvalidSection
	}
	for loop.ok && len(loop.b) > 0 {
		stream := TLVNITStream{TLVStreamID: loop.u16(), OriginalNetworkID: loop.u16()}
		descriptors := loop.bytes(int(loop.u16() & 0x0fff))
		if err := loop.err(); err != nil {
			return nil, err
		}
		if stream.Descriptors, err = parseTLVDescriptors(descriptors); err != nil {
			return nil, err
		}
		nit.TLVStreams = append(nit.TLVStreams, stream)
	}
	return nit, loop.err()
}

// ServiceListEntry is one service of a TLV-SI service list descriptor.
type ServiceListEntry struct {
	ServiceID   uint16
	ServiceType byte
}

// ParseServiceListDescriptor parses a TLV-SI service list descriptor (0x41).
func ParseServiceListDescriptor(d TLVDescriptor) ([]ServiceListEntry, error) {
	if d.Tag != TLVDescriptorTagServiceList || len(d.Data)%3 != 0 {
		return nil, ErrInvalidSection
	}
	services := make([]ServiceListEntry, 0, len(d.Data)/3)
	for b := d.Data; len(b) > 0; b = b[3:] {
		services = append(services, ServiceListEntry{ServiceID: uint16(b[0])<<8 | uint16(b[1]), ServiceType: b[2]})
	}
	return services, nil
}

// RemoteControlKey assigns a service to a one-touch button.
type RemoteControlKey struct {
	RemoteControlKeyID byte
	ServiceID          uint16
}

// ParseRemoteControlKeyDescriptor parses a TLV-SI remote control key
// descriptor (0xCD) from the first loop of TLV-NIT.
func ParseRemoteControlKeyDescriptor(d TLVDescriptor) ([]RemoteControlKey, error) {
	if d.Tag != TLVDescriptorTagRemoteControlKey || len(d.Data) < 1 || len(d.Data) != 1+int(d.Data[0])*5 {
		return nil, ErrInvalidSection
	}
	keys := make([]RemoteControlKey, 0, d.Data[0])
	for b := d.Data[1:]; len(b) > 0; b = b[5:] {
		keys = append(keys, RemoteControlKey{RemoteControlKeyID: b[0], ServiceID: uint16(b[1])<<8 | uint16(b[2])})
	}
	return keys, nil
}
