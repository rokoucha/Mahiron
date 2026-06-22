package ts

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
	DescriptorTagCA                        = 0x09
	DescriptorTagService                   = 0x48
	DescriptorTagServiceList               = 0x41
	DescriptorTagShortEvent                = 0x4D
	DescriptorTagExtendedEvent             = 0x4E
	DescriptorTagComponent                 = 0x50
	DescriptorTagContent                   = 0x54
	DescriptorTagAudioComponent            = 0xC4
	DescriptorTagLogoTransmission          = 0xCF
	DescriptorTagTerrestrialDeliverySystem = 0xCD
	DescriptorTagEventGroup                = 0xD6
	DescriptorTagSeries                    = 0xD5
)
