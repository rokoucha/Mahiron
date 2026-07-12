package ts

import "encoding/binary"

const (
	DSMCCModuleDescriptorType                  = 0x01
	DSMCCModuleDescriptorName                  = 0x02
	DSMCCModuleDescriptorCRC32                 = 0x05
	DSMCCModuleDescriptorEstimatedDownloadTime = 0x07
	DSMCCModuleDescriptorCachingPriority       = 0x71
	DSMCCModuleDescriptorExpire                = 0xc0
	DSMCCModuleDescriptorActivationTime        = 0xc1
	DSMCCModuleDescriptorCompressionType       = 0xc2
)

// DSMCCModuleMetadata contains the standardized DII module information used
// to select and cache data-broadcast resources. Unknown descriptors remain in
// DSMCCModuleInfo.Info and are intentionally ignored here.
type DSMCCModuleMetadata struct {
	Type                     string
	Name                     string
	CRC32                    *uint32
	EstimatedDownloadSeconds *uint32
	CachingPriority          *byte
	ExpireMode               *byte
	ExpireData               []byte
	ActivationMode           *byte
	ActivationData           []byte
	CompressionType          *byte
	OriginalSize             *uint32
}

// Metadata parses the descriptor loop in moduleInfoByte. A malformed trailing
// descriptor makes ok false so callers never act on partially trusted cache
// metadata.
func (m DSMCCModuleInfo) Metadata() (metadata DSMCCModuleMetadata, ok bool) {
	for off := 0; off < len(m.Info); {
		if off+2 > len(m.Info) {
			return DSMCCModuleMetadata{}, false
		}
		tag, size := m.Info[off], int(m.Info[off+1])
		off += 2
		if off+size > len(m.Info) {
			return DSMCCModuleMetadata{}, false
		}
		data := m.Info[off : off+size]
		off += size
		switch tag {
		case DSMCCModuleDescriptorType:
			metadata.Type = string(data)
		case DSMCCModuleDescriptorName:
			metadata.Name = string(data)
		case DSMCCModuleDescriptorCRC32:
			if len(data) == 4 {
				metadata.CRC32 = ptrValue(binary.BigEndian.Uint32(data))
			}
		case DSMCCModuleDescriptorEstimatedDownloadTime:
			if len(data) == 4 {
				metadata.EstimatedDownloadSeconds = ptrValue(binary.BigEndian.Uint32(data))
			}
		case DSMCCModuleDescriptorCachingPriority:
			if len(data) >= 1 {
				metadata.CachingPriority = ptrValue(data[0])
			}
		case DSMCCModuleDescriptorExpire:
			if len(data) >= 1 {
				metadata.ExpireMode = ptrValue(data[0])
				metadata.ExpireData = append([]byte(nil), data[1:]...)
			}
		case DSMCCModuleDescriptorActivationTime:
			if len(data) >= 1 {
				metadata.ActivationMode = ptrValue(data[0])
				metadata.ActivationData = append([]byte(nil), data[1:]...)
			}
		case DSMCCModuleDescriptorCompressionType:
			if len(data) == 5 {
				metadata.CompressionType = ptrValue(data[0])
				metadata.OriginalSize = ptrValue(binary.BigEndian.Uint32(data[1:]))
			}
		}
	}
	return metadata, true
}

func ptrValue[T any](value T) *T { return &value }
