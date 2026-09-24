package channel

import (
	"sort"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/ts"
)

// serviceScan incrementally builds the service list from PAT, SDT and NIT
// sections supplied by a shared Demuxer. It is the TS service scan moved
// out of the ts package: the section parsers stay in ts, while the
// combination into services and the conversion to the internal broadcast
// model live here, where the TS system knowledge belongs.
type serviceScan struct {
	pat         *ts.PAT
	patSections *sectionSet
	nitReady    bool
	nitSections *sectionSet
	sdtReady    bool
	sdtSections *sectionSet
	services    map[uint16]*model.Service
	remoteKeys  map[uint16]uint8
}

func newServiceScan() *serviceScan {
	return &serviceScan{
		patSections: newSectionSet(),
		nitSections: newSectionSet(),
		sdtSections: newSectionSet(),
		services:    map[uint16]*model.Service{},
		remoteKeys:  map[uint16]uint8{},
	}
}

// sectionSet collects every section of one table version. Reset and
// readiness tracking reuse isdb.TableTracker; the raw sections stay here
// because the scan re-parses the whole table once it is complete.
type sectionSet struct {
	tracker  isdb.TableTracker
	sections map[byte]ts.Section
}

func newSectionSet() *sectionSet {
	return &sectionSet{sections: map[byte]ts.Section{}}
}

// add records one section, reporting whether the table version reset and
// whether every section through last_section_number has arrived.
func (s *sectionSet) add(section ts.Section) (reset bool, ready bool) {
	header, err := ts.ParseSectionHeader(section)
	if err != nil || !header.CurrentNextIndicator || header.SectionNumber > header.LastSectionNumber {
		return false, false
	}
	reset, _ = s.tracker.Add(isdb.SectionHeader{
		TableIDExtension:  header.TransportStreamID,
		Version:           header.VersionNumber,
		SectionNumber:     header.SectionNumber,
		LastSectionNumber: header.LastSectionNumber,
		CurrentNext:       header.CurrentNextIndicator,
	})
	if reset {
		s.sections = make(map[byte]ts.Section)
	}
	s.sections[header.SectionNumber] = section
	for number := byte(0); ; number++ {
		if _, ok := s.sections[number]; !ok {
			return reset, false
		}
		if number == header.LastSectionNumber {
			return reset, true
		}
	}
}

func (s *sectionSet) ordered() []ts.Section {
	numbers := make([]int, 0, len(s.sections))
	for number := range s.sections {
		numbers = append(numbers, int(number))
	}
	sort.Ints(numbers)
	sections := make([]ts.Section, 0, len(numbers))
	for _, number := range numbers {
		sections = append(sections, s.sections[byte(number)])
	}
	return sections
}

// Observe adds one complete section to the scan state.
func (s *serviceScan) Observe(section ts.Section) {
	switch section.TableID() {
	case ts.TableIDPAT:
		reset, ready := s.patSections.add(section)
		if reset {
			s.pat = nil
		}
		if ready {
			s.handlePAT()
		}
	case ts.TableIDSDT0:
		reset, ready := s.sdtSections.add(section)
		if reset {
			s.sdtReady = false
			s.services = map[uint16]*model.Service{}
		}
		if ready {
			s.handleSDT()
		}
	case ts.TableIDNIT0:
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

func (s *serviceScan) handlePAT() {
	var combined *ts.PAT
	for _, section := range s.patSections.ordered() {
		pat, err := ts.ParsePAT(section)
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

func (s *serviceScan) handleSDT() {
	services := map[uint16]*model.Service{}
	type directLogo struct {
		version        uint16
		downloadDataID uint16
	}
	directLogos := map[uint16]directLogo{}
	indirectServices := map[uint16]uint16{}
	for _, section := range s.sdtSections.ordered() {
		sdt, err := ts.ParseSDT(section)
		if err != nil {
			return
		}
		for _, svc := range sdt.Services {
			desc := serviceDescriptorFromDescriptors(svc.Descriptors)
			if desc == nil {
				continue
			}
			info := &model.Service{
				Key: model.ServiceKey{
					NetworkID: sdt.OriginalNetworkID,
					StreamID:  sdt.TransportStreamID,
					ServiceID: svc.ServiceID,
				},
				Name:             desc.ServiceName,
				ProviderName:     desc.ServiceProviderName,
				Type:             desc.ServiceType,
				EITSchedule:      svc.EITScheduleFlag,
				EITPresentFollow: svc.EITPresentFollowing,
			}
			if logo := ts.LogoDescriptorFromDescriptors(svc.Descriptors); logo != nil {
				ref := &model.LogoRef{LogoID: logo.LogoID}
				switch logo.TransmissionType {
				case ts.LogoTransmissionTypeCDTDirect:
					version := logo.LogoVersion
					downloadDataID := logo.DownloadDataID
					ref.Version = &version
					ref.DownloadDataID = &downloadDataID
					directLogos[logo.LogoID] = directLogo{version: logo.LogoVersion, downloadDataID: logo.DownloadDataID}
				case ts.LogoTransmissionTypeCDTIndirect:
					indirectServices[svc.ServiceID] = logo.LogoID
				}
				info.Logo = ref
			}
			services[svc.ServiceID] = info
		}
	}
	for serviceID, logoID := range indirectServices {
		logo, ok := directLogos[logoID]
		if !ok {
			continue
		}
		info, ok := services[serviceID]
		if !ok || info.Logo == nil {
			continue
		}
		version := logo.version
		downloadDataID := logo.downloadDataID
		info.Logo.Version = &version
		info.Logo.DownloadDataID = &downloadDataID
	}
	s.services = services
	s.sdtReady = true
	s.applyRemoteKeys()
}

func serviceDescriptorFromDescriptors(descriptors []ts.Descriptor) *ts.ServiceDescriptor {
	for _, desc := range descriptors {
		if desc.Tag() != ts.DescriptorTagService {
			continue
		}
		service, err := ts.ParseServiceDescriptor(desc)
		if err == nil {
			return service
		}
	}
	return nil
}

func (s *serviceScan) handleNIT() {
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

func (s *serviceScan) applyRemoteKeys() {
	for _, info := range s.services {
		info.RemoteControlKey = nil
		if key, ok := s.remoteKeys[info.Key.StreamID]; ok {
			key := key
			info.RemoteControlKey = &key
		}
	}
}

func remoteKeysFromNIT(section ts.Section) map[uint16]uint8 {
	keys := map[uint16]uint8{}
	nit, err := ts.ParseNIT(section)
	if err != nil || nit.TableID != ts.TableIDNIT0 {
		return keys
	}
	for _, transportStream := range nit.TransportStreams {
		for _, desc := range transportStream.Descriptors {
			if desc.Tag() == ts.DescriptorTagTSInformation {
				info, err := ts.ParseTSInformationDescriptor(desc)
				if err == nil {
					keys[transportStream.TransportStreamID] = info.RemoteControlKeyID
				}
			}
		}
	}
	return keys
}

// Complete reports whether complete current PAT, SDT and NIT tables arrived.
func (s *serviceScan) Complete() bool { return s.pat != nil && s.nitReady && s.sdtReady }

// Services returns the currently assembled service list in service ID order,
// limited to services the PAT carries.
func (s *serviceScan) Services() []model.Service {
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
	services := make([]model.Service, 0, len(serviceIDs))
	for _, serviceID := range serviceIDs {
		services = append(services, *s.services[uint16(serviceID)])
	}
	return services
}
