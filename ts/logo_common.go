package ts

import (
	"bytes"
	"encoding/binary"
)

const (
	StreamTypeDSMCCDataCarousel = 0x0D

	// The BS all-receiver common data service (service_id 929, ARIB TR-B15
	// Part 1 5.2.1) currently rides the NHK BS transport stream on BS-15
	// (TSID 0x40F1). Used only as a bootstrap hint until an SDTT
	// announcement reveals the actual location.
	DefaultCommonLogoOriginalNetworkID uint16 = 0x0004
	DefaultCommonLogoTransportStreamID uint16 = 0x40f1
	DefaultCommonLogoServiceID         uint16 = 929
	NetworkLogoTransportStreamWildcard uint16 = 0xffff
	NetworkLogoServiceWildcard         uint16 = 0xffff
)

type CommonLogoService struct {
	OriginalNetworkID uint16
	TransportStreamID uint16
	ServiceID         uint16
}

type CommonLogoImage struct {
	LogoID      uint16
	LogoType    byte
	LogoVersion uint16
	DownloadID  uint16
	Services    []CommonLogoService
	Data        []byte
	IsDeleted   bool
	IsNetwork   bool
	SourceLabel string
}

func ParseLogoDataModule(module []byte) ([]CommonLogoImage, error) {
	if len(module) < 3 {
		return nil, ErrInvalidSection
	}
	logoType := module[0]
	loops := int(binary.BigEndian.Uint16(module[1:3]))
	off := 3
	result := make([]CommonLogoImage, 0, loops)
	for i := 0; i < loops; i++ {
		if off+3 > len(module) {
			return nil, ErrInvalidSection
		}
		logoID := binary.BigEndian.Uint16(module[off:off+2]) & 0x01ff
		serviceCount := int(module[off+2])
		off += 3
		services := make([]CommonLogoService, 0, serviceCount)
		isNetwork := false
		for j := 0; j < serviceCount; j++ {
			if off+6 > len(module) {
				return nil, ErrInvalidSection
			}
			service := CommonLogoService{
				OriginalNetworkID: binary.BigEndian.Uint16(module[off : off+2]),
				TransportStreamID: binary.BigEndian.Uint16(module[off+2 : off+4]),
				ServiceID:         binary.BigEndian.Uint16(module[off+4 : off+6]),
			}
			off += 6
			if service.TransportStreamID == NetworkLogoTransportStreamWildcard && service.ServiceID == NetworkLogoServiceWildcard {
				isNetwork = true
			}
			services = append(services, service)
		}
		if off+2 > len(module) {
			return nil, ErrInvalidSection
		}
		size := int(binary.BigEndian.Uint16(module[off : off+2]))
		off += 2
		if off+size > len(module) {
			return nil, ErrInvalidSection
		}
		data := module[off : off+size]
		off += size
		if size > 0 && !bytes.HasPrefix(data, pngSignature) {
			return nil, ErrInvalidSection
		}
		result = append(result, CommonLogoImage{
			LogoID:    logoID,
			LogoType:  logoType,
			Services:  services,
			Data:      append([]byte(nil), data...),
			IsDeleted: size == 0,
			IsNetwork: isNetwork,
		})
	}
	if off != len(module) {
		return nil, ErrInvalidSection
	}
	return result, nil
}

type DSMCCModuleInfo struct {
	ModuleID   uint16
	ModuleSize uint32
	Version    byte
	Info       []byte
}

func (m DSMCCModuleInfo) IsLogoModule() bool {
	return bytes.Contains(m.Info, []byte("LOGO-0"))
}

func (m DSMCCModuleInfo) LogoType() (byte, bool) {
	idx := bytes.Index(m.Info, []byte("LOGO-0"))
	if idx < 0 || idx+7 > len(m.Info) {
		return 0, false
	}
	n := m.Info[idx+6]
	if n < '0' || n > '5' {
		return 0, false
	}
	return n - '0', true
}

type DSMCCDII struct {
	DownloadID uint32
	BlockSize  uint16
	Modules    []DSMCCModuleInfo
}

func ParseDSMCCDII(s Section) (*DSMCCDII, error) {
	if len(s) < 23 || s.TableID() != TableIDDSMCCDII || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	body, err := dsmccMessageBody(s, 0x1002)
	if err != nil {
		return nil, err
	}
	if len(body) < 18 {
		return nil, ErrInvalidSection
	}
	result := &DSMCCDII{
		DownloadID: binary.BigEndian.Uint32(body[0:4]),
		BlockSize:  binary.BigEndian.Uint16(body[4:6]),
	}
	compatLen := int(binary.BigEndian.Uint16(body[16:18]))
	off := 18 + compatLen
	if off+2 > len(body) {
		return nil, ErrInvalidSection
	}
	moduleCount := int(binary.BigEndian.Uint16(body[off : off+2]))
	off += 2
	for i := 0; i < moduleCount; i++ {
		if off+8 > len(body) {
			return nil, ErrInvalidSection
		}
		module := DSMCCModuleInfo{
			ModuleID:   binary.BigEndian.Uint16(body[off : off+2]),
			ModuleSize: binary.BigEndian.Uint32(body[off+2 : off+6]),
			Version:    body[off+6],
		}
		infoLen := int(body[off+7])
		off += 8
		if off+infoLen > len(body) {
			return nil, ErrInvalidSection
		}
		module.Info = append([]byte(nil), body[off:off+infoLen]...)
		off += infoLen
		result.Modules = append(result.Modules, module)
	}
	return result, nil
}

type DSMCCDDB struct {
	DownloadID    uint32
	ModuleID      uint16
	ModuleVersion byte
	BlockNumber   uint16
	Data          []byte
}

func ParseDSMCCDDB(s Section) (*DSMCCDDB, error) {
	if len(s) < 25 || s.TableID() != TableIDDSMCCDDB || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	body, err := dsmccMessageBody(s, 0x1003)
	if err != nil {
		return nil, err
	}
	if len(body) < 6 {
		return nil, ErrInvalidSection
	}
	// Unlike the DII message, the DDB message carries downloadId inside
	// dsmccDownloadDataHeader() and its payload starts at moduleId
	// (ARIB STD-B24 Part 3 Tables 6-21 and 6-22).
	return &DSMCCDDB{
		DownloadID:    binary.BigEndian.Uint32(s[12:16]),
		ModuleID:      binary.BigEndian.Uint16(body[0:2]),
		ModuleVersion: body[2],
		BlockNumber:   binary.BigEndian.Uint16(body[4:6]),
		Data:          append([]byte(nil), body[6:]...),
	}, nil
}

func dsmccMessageBody(s Section, messageID uint16) ([]byte, error) {
	end := s.TotalLength() - 4
	if end < 20 {
		return nil, ErrInvalidSection
	}
	message := s[8:end]
	if len(message) < 12 || message[0] != 0x11 || message[1] != 0x03 || binary.BigEndian.Uint16(message[2:4]) != messageID {
		return nil, ErrInvalidSection
	}
	adaptationLen := int(message[9])
	messageLen := int(binary.BigEndian.Uint16(message[10:12]))
	off := 12 + adaptationLen
	if off > len(message) || off+messageLen > len(message) {
		return nil, ErrInvalidSection
	}
	return message[off : off+messageLen], nil
}

type CommonDataAnnouncement struct {
	OriginalNetworkID uint16
	TransportStreamID uint16
	ServiceID         uint16
	DownloadID        uint32
	VersionID         uint16
}

type DSMCCLogoCarousel struct {
	modules map[uint16]*dsmccLogoModuleState
}

type dsmccLogoModuleState struct {
	info       DSMCCModuleInfo
	downloadID uint32
	blockSize  uint16
	blocks     map[uint16][]byte
	emitted    bool
}

func NewDSMCCLogoCarousel() *DSMCCLogoCarousel {
	return &DSMCCLogoCarousel{modules: map[uint16]*dsmccLogoModuleState{}}
}

func (c *DSMCCLogoCarousel) ObserveDII(dii *DSMCCDII) {
	if c.modules == nil {
		c.modules = map[uint16]*dsmccLogoModuleState{}
	}
	for _, module := range dii.Modules {
		if !module.IsLogoModule() {
			continue
		}
		current := c.modules[module.ModuleID]
		if current == nil || current.downloadID != dii.DownloadID || current.info.Version != module.Version || current.info.ModuleSize != module.ModuleSize {
			c.modules[module.ModuleID] = &dsmccLogoModuleState{
				info:       module,
				downloadID: dii.DownloadID,
				blockSize:  dii.BlockSize,
				blocks:     map[uint16][]byte{},
			}
		}
	}
}

func (c *DSMCCLogoCarousel) ObserveDDB(ddb *DSMCCDDB) ([]CommonLogoImage, error) {
	if c == nil || c.modules == nil {
		return nil, nil
	}
	module := c.modules[ddb.ModuleID]
	if module == nil || module.emitted || module.downloadID != ddb.DownloadID || module.info.Version != ddb.ModuleVersion {
		return nil, nil
	}
	module.blocks[ddb.BlockNumber] = append([]byte(nil), ddb.Data...)
	data, ok := module.assemble()
	if !ok {
		return nil, nil
	}
	images, err := ParseLogoDataModule(data)
	if err != nil {
		return nil, err
	}
	if logoType, ok := module.info.LogoType(); ok {
		for i := range images {
			images[i].LogoType = logoType
			images[i].LogoVersion = uint16(module.info.Version)
			images[i].DownloadID = uint16(module.downloadID)
			images[i].SourceLabel = string(module.info.Info)
		}
	}
	module.emitted = true
	return images, nil
}

func (m *dsmccLogoModuleState) assemble() ([]byte, bool) {
	if m.blockSize == 0 {
		return nil, false
	}
	blockCount := int((m.info.ModuleSize + uint32(m.blockSize) - 1) / uint32(m.blockSize))
	if len(m.blocks) < blockCount {
		return nil, false
	}
	data := make([]byte, 0, m.info.ModuleSize)
	for i := 0; i < blockCount; i++ {
		block, ok := m.blocks[uint16(i)]
		if !ok {
			return nil, false
		}
		data = append(data, block...)
	}
	if uint32(len(data)) < m.info.ModuleSize {
		return nil, false
	}
	return data[:m.info.ModuleSize], true
}

func ParseSDTTCommonDataAnnouncements(s Section) ([]CommonDataAnnouncement, error) {
	if len(s) < 12 || s.TableID() != TableIDSDTT || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	header, err := ParseSectionHeader(s)
	if err != nil {
		return nil, err
	}
	if !header.CurrentNextIndicator || header.SectionNumber != 0 || header.TableID != TableIDSDTT {
		return nil, nil
	}
	tableIDExt := binary.BigEndian.Uint16(s[3:5])
	if !isSatelliteCommonDataTableIDExt(tableIDExt) {
		return nil, nil
	}
	end := s.TotalLength() - 4
	if end < 17 {
		return nil, ErrInvalidSection
	}
	tsid := binary.BigEndian.Uint16(s[8:10])
	onid := binary.BigEndian.Uint16(s[10:12])
	sid := binary.BigEndian.Uint16(s[12:14])
	if !IsSatelliteOriginalNetworkID(onid) {
		return nil, nil
	}
	contentCount := int(s[14])
	off := 15
	result := make([]CommonDataAnnouncement, 0, contentCount)
	for i := 0; i < contentCount; i++ {
		if off+8 > end {
			return nil, ErrInvalidSection
		}
		versionID := uint16(s[off+2])<<4 | uint16(s[off+3]>>4)
		contentDescriptionLength := int(uint16(s[off+4])<<4 | uint16(s[off+5]>>4))
		scheduleDescriptionLength := int(uint16(s[off+6])<<4 | uint16(s[off+7]>>4))
		descriptorOff := off + 8 + scheduleDescriptionLength
		contentEnd := off + 8 + contentDescriptionLength
		if contentEnd > end {
			return nil, ErrInvalidSection
		}
		if descriptorOff > contentEnd {
			return nil, ErrInvalidSection
		}
		downloadID, ok, err := sdttDownloadContentID(s[descriptorOff:contentEnd])
		if err != nil {
			return nil, err
		}
		if ok && scheduleDescriptionLength == 0 && sid != 0 && sid != 0xffff {
			result = append(result, CommonDataAnnouncement{
				OriginalNetworkID: onid,
				TransportStreamID: tsid,
				ServiceID:         sid,
				DownloadID:        downloadID,
				VersionID:         versionID,
			})
		}
		off = contentEnd
	}
	return result, nil
}

func isSatelliteCommonDataTableIDExt(tableIDExt uint16) bool {
	return tableIDExt == 0xfffe || tableIDExt == 0xfffc
}

func sdttDownloadContentID(descriptors []byte) (uint32, bool, error) {
	for off := 0; off < len(descriptors); {
		if off+2 > len(descriptors) {
			return 0, false, ErrInvalidSection
		}
		tag := descriptors[off]
		length := int(descriptors[off+1])
		off += 2
		if off+length > len(descriptors) {
			return 0, false, ErrInvalidSection
		}
		data := descriptors[off : off+length]
		off += length
		if tag != DescriptorTagDownloadContent {
			continue
		}
		if len(data) < 18 {
			return 0, false, ErrInvalidSection
		}
		flags := data[0]
		compatibilityFlag := flags&0x20 != 0
		moduleInfoFlag := flags&0x10 != 0
		textInfoFlag := flags&0x08 != 0
		downloadID := binary.BigEndian.Uint32(data[5:9])
		pos := 18
		if compatibilityFlag {
			if pos+2 > len(data) {
				return 0, false, ErrInvalidSection
			}
			length := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2 + length
			if pos > len(data) {
				return 0, false, ErrInvalidSection
			}
		}
		if moduleInfoFlag {
			if pos+1 > len(data) {
				return 0, false, ErrInvalidSection
			}
			count := int(data[pos])
			pos++
			for i := 0; i < count; i++ {
				if pos+7 > len(data) {
					return 0, false, ErrInvalidSection
				}
				infoLen := int(data[pos+6])
				pos += 7 + infoLen
				if pos > len(data) {
					return 0, false, ErrInvalidSection
				}
			}
		}
		if pos+1 > len(data) {
			return 0, false, ErrInvalidSection
		}
		privateLen := int(data[pos])
		pos += 1 + privateLen
		if pos > len(data) {
			return 0, false, ErrInvalidSection
		}
		if textInfoFlag {
			if pos+4 > len(data) {
				return 0, false, ErrInvalidSection
			}
			textLen := int(data[pos+3])
			pos += 4 + textLen
			if pos > len(data) {
				return 0, false, ErrInvalidSection
			}
		}
		return downloadID, true, nil
	}
	return 0, false, nil
}

func DefaultCommonDataAnnouncement() CommonDataAnnouncement {
	return CommonDataAnnouncement{
		OriginalNetworkID: DefaultCommonLogoOriginalNetworkID,
		TransportStreamID: DefaultCommonLogoTransportStreamID,
		ServiceID:         DefaultCommonLogoServiceID,
	}
}

func IsSatelliteOriginalNetworkID(onid uint16) bool {
	return onid == 0x0004 || onid == 0x0006 || onid == 0x0007
}
