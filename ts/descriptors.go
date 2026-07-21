package ts

import "fmt"

// Descriptor represents a raw MPEG-2 descriptor.
type Descriptor []byte

// Tag returns the descriptor_tag.
func (d Descriptor) Tag() byte { return d[0] }

// Length returns the descriptor_length.
func (d Descriptor) Length() int { return int(d[1]) }

// Data returns the descriptor payload bytes (after tag and length).
func (d Descriptor) Data() []byte { return d[2 : 2+d.Length()] }

// ParseDescriptors parses a sequence of descriptors from bytes.
func ParseDescriptors(b []byte) []Descriptor {
	var descriptors []Descriptor
	for len(b) >= 2 {
		length := int(b[1])
		if len(b) < 2+length {
			break
		}
		descriptors = append(descriptors, Descriptor(b[:2+length]))
		b = b[2+length:]
	}
	return descriptors
}

// ARIB descriptor tags.
const (
	DescriptorTagCA                  = 0x09
	DescriptorTagService             = 0x48
	DescriptorTagServiceList         = 0x41
	DescriptorTagShortEvent          = 0x4D
	DescriptorTagExtendedEvent       = 0x4E
	DescriptorTagComponent           = 0x50
	DescriptorTagContent             = 0x54
	DescriptorTagAudioComponent      = 0xC4
	DescriptorTagDataComponent       = 0xFD
	DescriptorTagExtendedBroadcaster = 0xCE
	DescriptorTagBroadcasterName     = 0xD8
	DescriptorTagDownloadContent     = 0xC9
	DescriptorTagLogoTransmission    = 0xCF
	// TS information descriptor carries remote_control_key_id in terrestrial NIT TS loops.
	DescriptorTagTSInformation = 0xCD
	// Terrestrial delivery system descriptor is assigned by the terrestrial operating guidelines.
	DescriptorTagTerrestrialDeliverySystem = 0xFA
	DescriptorTagEventGroup                = 0xD6
	DescriptorTagSeries                    = 0xD5
)

// DataComponentDescriptor identifies the data coding scheme carried by one
// PMT elementary stream. ARIB BML uses the additional data below it to
// describe its entry point and carousel behavior.
type DataComponentDescriptor struct {
	DataComponentID             uint16
	AdditionalDataComponentInfo []byte
}

type AdditionalAribBXMLInfo struct {
	TransmissionFormat         byte
	EntryPointFlag             bool
	EntryPointInfo             *AdditionalAribBXMLEntryPointInfo
	AdditionalAribCarouselInfo *AdditionalAribCarouselInfo
}

type AdditionalAribBXMLEntryPointInfo struct {
	AutoStartFlag      bool
	DocumentResolution byte
	UseXML             bool
	DefaultVersionFlag bool
	IndependentFlag    bool
	StyleForTVFlag     bool
	BMLMajorVersion    uint16
	BMLMinorVersion    uint16
	BXMLMajorVersion   *uint16
	BXMLMinorVersion   *uint16
}

type AdditionalAribCarouselInfo struct {
	DataEventID           byte
	EventSectionFlag      bool
	OnDemandRetrievalFlag bool
	FileStorableFlag      bool
	StartPriority         byte
}

func ParseDataComponentDescriptor(d Descriptor) (*DataComponentDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() {
		return nil, ErrInvalidSection
	}
	if d.Tag() != DescriptorTagDataComponent || len(d.Data()) < 2 {
		return nil, ErrInvalidSection
	}
	data := d.Data()
	return &DataComponentDescriptor{
		DataComponentID:             uint16(data[0])<<8 | uint16(data[1]),
		AdditionalDataComponentInfo: append([]byte(nil), data[2:]...),
	}, nil
}

// ParseAdditionalAribBXMLInfo parses the additional_data_component_info for
// ARIB data component IDs 0x0007, 0x000b, 0x000c, and 0x000d.
func ParseAdditionalAribBXMLInfo(data []byte) (*AdditionalAribBXMLInfo, error) {
	if len(data) < 3 {
		return nil, ErrInvalidSection
	}
	info := &AdditionalAribBXMLInfo{
		TransmissionFormat: (data[0] >> 6) & 0x03,
		EntryPointFlag:     data[0]&0x20 != 0,
	}
	off := 1
	if info.EntryPointFlag {
		if len(data) < off+1 {
			return nil, ErrInvalidSection
		}
		entry := &AdditionalAribBXMLEntryPointInfo{
			AutoStartFlag:      data[0]&0x10 != 0,
			DocumentResolution: data[0] & 0x0f,
			UseXML:             data[off]&0x80 != 0,
			DefaultVersionFlag: data[off]&0x40 != 0,
			IndependentFlag:    data[off]&0x20 != 0,
			StyleForTVFlag:     data[off]&0x10 != 0,
			BMLMajorVersion:    1,
			BMLMinorVersion:    0,
		}
		off++
		if !entry.DefaultVersionFlag {
			if len(data) < off+4 {
				return nil, ErrInvalidSection
			}
			entry.BMLMajorVersion = uint16(data[off])<<8 | uint16(data[off+1])
			entry.BMLMinorVersion = uint16(data[off+2])<<8 | uint16(data[off+3])
			off += 4
			if entry.UseXML {
				if len(data) < off+4 {
					return nil, ErrInvalidSection
				}
				major := uint16(data[off])<<8 | uint16(data[off+1])
				minor := uint16(data[off+2])<<8 | uint16(data[off+3])
				entry.BXMLMajorVersion, entry.BXMLMinorVersion = &major, &minor
				off += 4
			}
		}
		info.EntryPointInfo = entry
	}
	if info.TransmissionFormat == 0 {
		if len(data) < off+2 {
			return nil, ErrInvalidSection
		}
		info.AdditionalAribCarouselInfo = &AdditionalAribCarouselInfo{
			DataEventID:           data[off] >> 4,
			EventSectionFlag:      data[off]&0x08 != 0,
			OnDemandRetrievalFlag: data[off+1]&0x80 != 0,
			FileStorableFlag:      data[off+1]&0x40 != 0,
			StartPriority:         (data[off+1] >> 5) & 0x01,
		}
	}
	return info, nil
}

type TSInformationTransmissionType struct {
	TransmissionTypeInfo byte
	ServiceIDs           []uint16
}

type TSInformationDescriptor struct {
	RemoteControlKeyID uint8
	TSName             string
	TransmissionTypes  []TSInformationTransmissionType
}

func ParseTSInformationDescriptor(d Descriptor) (*TSInformationDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() {
		return nil, ErrInvalidSection
	}
	if d.Tag() != DescriptorTagTSInformation {
		return nil, fmt.Errorf("ts: unexpected descriptor tag %#02x", d.Tag())
	}
	data := d.Data()
	if len(data) < 2 {
		return nil, ErrInvalidSection
	}
	nameLen := int(data[1] >> 2)
	transmissionTypeCount := int(data[1] & 0x03)
	nameStart := 2
	nameEnd := nameStart + nameLen
	if nameEnd > len(data) {
		return nil, ErrInvalidSection
	}
	name, err := DecodeARIBString(data[nameStart:nameEnd])
	if err != nil {
		return nil, err
	}
	result := &TSInformationDescriptor{
		RemoteControlKeyID: data[0],
		TSName:             name,
		TransmissionTypes:  make([]TSInformationTransmissionType, 0, transmissionTypeCount),
	}
	off := nameEnd
	for range transmissionTypeCount {
		if off+2 > len(data) {
			return nil, ErrInvalidSection
		}
		item := TSInformationTransmissionType{TransmissionTypeInfo: data[off]}
		numServices := int(data[off+1])
		off += 2
		if off+numServices*2 > len(data) {
			return nil, ErrInvalidSection
		}
		item.ServiceIDs = make([]uint16, 0, numServices)
		for range numServices {
			item.ServiceIDs = append(item.ServiceIDs, uint16(data[off])<<8|uint16(data[off+1]))
			off += 2
		}
		result.TransmissionTypes = append(result.TransmissionTypes, item)
	}
	return result, nil
}

type ServiceListEntry struct {
	ServiceID   uint16
	ServiceType uint8
}

type ServiceListDescriptor struct {
	Services []ServiceListEntry
}

func ParseServiceListDescriptor(d Descriptor) (*ServiceListDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() {
		return nil, ErrInvalidSection
	}
	if d.Tag() != DescriptorTagServiceList {
		return nil, fmt.Errorf("ts: unexpected descriptor tag %#02x", d.Tag())
	}
	data := d.Data()
	if len(data)%3 != 0 {
		return nil, ErrInvalidSection
	}
	result := &ServiceListDescriptor{
		Services: make([]ServiceListEntry, 0, len(data)/3),
	}
	for off := 0; off < len(data); off += 3 {
		result.Services = append(result.Services, ServiceListEntry{
			ServiceID:   uint16(data[off])<<8 | uint16(data[off+1]),
			ServiceType: data[off+2],
		})
	}
	return result, nil
}

type TerrestrialDeliverySystemDescriptor struct {
	AreaCode         uint16
	GuardInterval    byte
	TransmissionMode byte
	Frequencies      []uint16
}

func ParseTerrestrialDeliverySystemDescriptor(d Descriptor) (*TerrestrialDeliverySystemDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() {
		return nil, ErrInvalidSection
	}
	if d.Tag() != DescriptorTagTerrestrialDeliverySystem {
		return nil, fmt.Errorf("ts: unexpected descriptor tag %#02x", d.Tag())
	}
	data := d.Data()
	if len(data) < 2 || len(data[2:])%2 != 0 {
		return nil, ErrInvalidSection
	}
	result := &TerrestrialDeliverySystemDescriptor{
		AreaCode:         uint16(data[0])<<4 | uint16(data[1]>>4),
		GuardInterval:    (data[1] >> 2) & 0x03,
		TransmissionMode: data[1] & 0x03,
		Frequencies:      make([]uint16, 0, len(data[2:])/2),
	}
	for off := 2; off < len(data); off += 2 {
		result.Frequencies = append(result.Frequencies, uint16(data[off])<<8|uint16(data[off+1]))
	}
	return result, nil
}
