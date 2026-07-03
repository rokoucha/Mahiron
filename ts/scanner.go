package ts

import (
	"sort"
)

// ServiceInfo represents a scanned service in the scanner JSON output.
type ServiceInfo struct {
	Nid                 uint16  `json:"nid"`
	Tsid                uint16  `json:"tsid"`
	Sid                 uint16  `json:"sid"`
	Name                string  `json:"name"`
	Type                uint8   `json:"type"`
	EITScheduleFlag     bool    `json:"eitScheduleFlag"`
	EITPresentFollowing bool    `json:"eitPresentFollowing"`
	LogoId              int64   `json:"logoId"`
	LogoVersion         *uint16 `json:"logoVersion,omitempty"`
	LogoDownloadDataId  *uint16 `json:"logoDownloadDataId,omitempty"`
	RemoteControlKeyId  *uint8  `json:"remoteControlKeyId,omitempty"`
}

// ServiceScan incrementally builds the service list from PAT, SDT and NIT
// sections supplied by a shared Demuxer.
type ServiceScan struct {
	state *serviceScanState
}

// NewServiceScan creates an incremental service scan.
func NewServiceScan() *ServiceScan { return &ServiceScan{state: newServiceScanState()} }

// Observe adds one complete section to the scan state.
func (s *ServiceScan) Observe(section Section) {
	if s != nil && s.state != nil {
		s.state.observeSection(section)
	}
}

// Complete reports whether complete current PAT, SDT and NIT tables arrived.
func (s *ServiceScan) Complete() bool { return s != nil && s.state != nil && s.state.complete() }

// Services returns the currently assembled service list.
func (s *ServiceScan) Services() []ServiceInfo {
	if s == nil || s.state == nil {
		return nil
	}
	return s.state.serviceList()
}

type serviceScanState struct {
	pat         *PAT
	patSections tableSectionSet
	nitReady    bool
	nitSections tableSectionSet
	sdtReady    bool
	sdtSections tableSectionSet
	services    map[uint16]ServiceInfo
	remoteKeys  map[uint16]uint8
}

func newServiceScanState() *serviceScanState {
	return &serviceScanState{
		services:   map[uint16]ServiceInfo{},
		remoteKeys: map[uint16]uint8{},
	}
}

// tableSectionSet collects all sections belonging to one version of a PSI/SI
// table. A table is ready only after every section through last_section_number
// has arrived.
type tableSectionSet struct {
	initialized bool
	extension   uint16
	version     byte
	last        byte
	sections    map[byte]Section
}

func (s *tableSectionSet) add(section Section) (reset bool, ready bool) {
	header, err := ParseSectionHeader(section)
	if err != nil || !header.CurrentNextIndicator || header.SectionNumber > header.LastSectionNumber {
		return false, false
	}
	if !s.initialized || s.extension != header.TransportStreamID || s.version != header.VersionNumber || s.last != header.LastSectionNumber {
		s.initialized = true
		s.extension = header.TransportStreamID
		s.version = header.VersionNumber
		s.last = header.LastSectionNumber
		s.sections = make(map[byte]Section, int(s.last)+1)
		reset = true
	}
	s.sections[header.SectionNumber] = section
	if len(s.sections) != int(s.last)+1 {
		return reset, false
	}
	for number := 0; number <= int(s.last); number++ {
		if _, ok := s.sections[byte(number)]; !ok {
			return reset, false
		}
	}
	return reset, true
}

func (s *tableSectionSet) ordered() []Section {
	sections := make([]Section, 0, int(s.last)+1)
	for number := 0; number <= int(s.last); number++ {
		sections = append(sections, s.sections[byte(number)])
	}
	return sections
}

func (s *serviceScanState) observeSection(section Section) {
	switch section.TableID() {
	case TableIDPAT:
		reset, ready := s.patSections.add(section)
		if reset {
			s.pat = nil
		}
		if ready {
			s.handlePAT()
		}
	case TableIDSDT0:
		reset, ready := s.sdtSections.add(section)
		if reset {
			s.sdtReady = false
			s.services = map[uint16]ServiceInfo{}
		}
		if ready {
			s.handleSDT()
		}
	case TableIDNIT0:
		reset, ready := s.nitSections.add(section)
		if reset {
			s.nitReady = false
			s.remoteKeys = map[uint16]uint8{}
			s.applyRemoteKeys()
		}
		if ready {
			s.handleNIT()
		}
	}
}

func (s *serviceScanState) handlePAT() {
	var combined *PAT
	for _, section := range s.patSections.ordered() {
		pat, err := ParsePAT(section)
		if err != nil {
			return
		}
		if combined == nil {
			combined = pat
			continue
		}
		for serviceID, pmtPID := range pat.Programs {
			combined.Programs[serviceID] = pmtPID
		}
	}
	s.pat = combined
}

func (s *serviceScanState) handleSDT() {
	services := map[uint16]ServiceInfo{}
	type directLogo struct {
		version        uint16
		downloadDataID uint16
	}
	directLogos := map[uint16]directLogo{}
	indirectServices := map[uint16]uint16{}
	for _, section := range s.sdtSections.ordered() {
		sdt, err := ParseSDT(section)
		if err != nil {
			return
		}
		for _, svc := range sdt.Services {
			desc := serviceDescriptorFromDescriptors(svc.Descriptors)
			if desc == nil {
				continue
			}
			info := ServiceInfo{
				Nid:                 sdt.OriginalNetworkID,
				Tsid:                sdt.TransportStreamID,
				Sid:                 svc.ServiceID,
				Name:                desc.ServiceName,
				Type:                desc.ServiceType,
				EITScheduleFlag:     svc.EITScheduleFlag,
				EITPresentFollowing: svc.EITPresentFollowing,
				LogoId:              -1,
			}
			if logo := LogoDescriptorFromDescriptors(svc.Descriptors); logo != nil {
				info.LogoId = int64(logo.LogoID)
				switch logo.TransmissionType {
				case LogoTransmissionTypeCDTDirect:
					info.LogoVersion = uint16Ptr(logo.LogoVersion)
					info.LogoDownloadDataId = uint16Ptr(logo.DownloadDataID)
					directLogos[logo.LogoID] = directLogo{version: logo.LogoVersion, downloadDataID: logo.DownloadDataID}
				case LogoTransmissionTypeCDTIndirect:
					indirectServices[svc.ServiceID] = logo.LogoID
				}
			}
			services[svc.ServiceID] = info
		}
	}
	for serviceID, logoID := range indirectServices {
		logo, ok := directLogos[logoID]
		if !ok {
			continue
		}
		info := services[serviceID]
		info.LogoVersion = uint16Ptr(logo.version)
		info.LogoDownloadDataId = uint16Ptr(logo.downloadDataID)
		services[serviceID] = info
	}
	s.services = services
	s.sdtReady = true
	s.applyRemoteKeys()
}

func serviceDescriptorFromDescriptors(descriptors []Descriptor) *ServiceDescriptor {
	for _, desc := range descriptors {
		if desc.Tag() != DescriptorTagService {
			continue
		}
		service, err := ParseServiceDescriptor(desc)
		if err == nil {
			return service
		}
	}
	return nil
}

func (s *serviceScanState) handleNIT() {
	keys := map[uint16]uint8{}
	for _, section := range s.nitSections.ordered() {
		for tsid, key := range remoteKeysFromNIT(section) {
			keys[tsid] = key
		}
	}
	s.remoteKeys = keys
	s.nitReady = true
	s.applyRemoteKeys()
}

func (s *serviceScanState) applyRemoteKeys() {
	for sid, info := range s.services {
		info.RemoteControlKeyId = nil
		key, ok := s.remoteKeys[info.Tsid]
		if ok {
			info.RemoteControlKeyId = uint8Ptr(key)
		}
		s.services[sid] = info
	}
}

func remoteKeysFromNIT(section Section) map[uint16]uint8 {
	keys := map[uint16]uint8{}
	nit, err := ParseNIT(section)
	if err != nil || nit.TableID != TableIDNIT0 {
		return keys
	}
	for _, transportStream := range nit.TransportStreams {
		for _, desc := range transportStream.Descriptors {
			if desc.Tag() == DescriptorTagTSInformation {
				info, err := ParseTSInformationDescriptor(desc)
				if err == nil {
					keys[transportStream.TransportStreamID] = info.RemoteControlKeyID
				}
			}
		}
	}
	return keys
}

func (s *serviceScanState) complete() bool {
	return s.pat != nil && s.nitReady && s.sdtReady
}

func (s *serviceScanState) serviceList() []ServiceInfo {
	serviceIDs := make([]int, 0, len(s.services))
	if s.pat != nil {
		for serviceID := range s.pat.Programs {
			if _, ok := s.services[serviceID]; ok {
				serviceIDs = append(serviceIDs, int(serviceID))
			}
		}
	} else {
		for serviceID := range s.services {
			serviceIDs = append(serviceIDs, int(serviceID))
		}
	}
	sort.Ints(serviceIDs)
	services := make([]ServiceInfo, 0, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		services = append(services, s.services[uint16(serviceID)])
	}
	return services
}

func uint8Ptr(v uint8) *uint8 {
	return &v
}

func uint16Ptr(v uint16) *uint16 {
	return &v
}
