package tlv

import (
	"sort"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/mmt"
)

// serviceScan builds a TLV stream's services from MH-SDT[actual] only (not
// TLV-NIT or MH-SDT[other], which list the whole network on every stream),
// plus the remote control keys of TLV-NIT[actual].
type serviceScan struct {
	sdt      sectionSet
	nit      sectionSet
	services map[uint16]*model.Service
	keys     map[uint16]uint8
	sdtReady bool
	nitReady bool
}

func newServiceScan() *serviceScan {
	return &serviceScan{services: map[uint16]*model.Service{}, keys: map[uint16]uint8{}}
}

// sectionSet collects every section of one table version.
type sectionSet struct {
	tracker  isdb.TableTracker
	sections map[byte]mmt.Section
}

// add records a section and reports whether every section of its version
// arrived.
func (s *sectionSet) add(header mmt.SectionHeader, section mmt.Section) bool {
	if !header.CurrentNextIndicator {
		return false
	}
	reset, ready := s.tracker.Add(isdb.SectionHeader{
		TableIDExtension:  header.TableIDExtension,
		Version:           header.VersionNumber,
		SectionNumber:     header.SectionNumber,
		LastSectionNumber: header.LastSectionNumber,
		CurrentNext:       header.CurrentNextIndicator,
	})
	if reset || s.sections == nil {
		s.sections = map[byte]mmt.Section{}
	}
	s.sections[header.SectionNumber] = section
	return ready
}

func (s *sectionSet) ordered() []mmt.Section {
	numbers := make([]int, 0, len(s.sections))
	for number := range s.sections {
		numbers = append(numbers, int(number))
	}
	sort.Ints(numbers)
	sections := make([]mmt.Section, 0, len(numbers))
	for _, number := range numbers {
		sections = append(sections, s.sections[byte(number)])
	}
	return sections
}

// Observe adds one signal to the scan.
func (s *serviceScan) Observe(sig signal) {
	switch {
	case sig.TLVSI && len(sig.Section) > 0 && sig.Section.TableID() == mmt.TableIDTLVNITActual:
		nit, err := mmt.ParseTLVNIT(sig.Section)
		if err != nil {
			return
		}
		if s.nit.add(nit.SectionHeader, sig.Section) {
			s.handleNIT()
		}
	case !sig.TLVSI && len(sig.Section) > 0 && sig.Section.TableID() == mmt.TableIDMHSDTActual:
		sdt, err := mmt.ParseMHSDT(sig.Section)
		if err != nil {
			return
		}
		if s.sdt.add(sdt.SectionHeader, sig.Section) {
			s.handleSDT()
		}
	}
}

func (s *serviceScan) handleNIT() {
	keys := map[uint16]uint8{}
	for _, section := range s.nit.ordered() {
		nit, err := mmt.ParseTLVNIT(section)
		if err != nil {
			return
		}
		for _, d := range nit.NetworkDescriptors {
			if d.Tag != mmt.TLVDescriptorTagRemoteControlKey {
				continue
			}
			entries, err := mmt.ParseRemoteControlKeyDescriptor(d)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				keys[entry.ServiceID] = entry.RemoteControlKeyID
			}
		}
	}
	s.keys = keys
	s.nitReady = true
	s.applyRemoteControlKeys()
}

func (s *serviceScan) handleSDT() {
	services := map[uint16]*model.Service{}
	// Logos referred to through logo_id (CDT type 2) take the version and
	// download data ID of a service that refers to the same logo directly.
	direct := map[uint16]*mmt.MHLogoTransmissionDescriptor{}
	indirect := map[uint16]uint16{}
	for _, section := range s.sdt.ordered() {
		sdt, err := mmt.ParseMHSDT(section)
		if err != nil {
			return
		}
		for _, entry := range sdt.Services {
			var service *model.Service
			var logo *mmt.MHLogoTransmissionDescriptor
			for _, d := range entry.Descriptors {
				switch d.Tag {
				case mmt.DescriptorTagMHService:
					desc, err := mmt.ParseMHServiceDescriptor(d)
					if err != nil {
						continue
					}
					service = &model.Service{
						Key:              model.ServiceKey{NetworkID: sdt.OriginalNetworkID, StreamID: sdt.TLVStreamID, ServiceID: entry.ServiceID},
						Name:             desc.ServiceName,
						ProviderName:     desc.ServiceProviderName,
						Type:             desc.ServiceType,
						RunningStatus:    entry.RunningStatus,
						FreeCA:           entry.FreeCAMode,
						EITSchedule:      entry.EITScheduleFlag,
						EITPresentFollow: entry.EITPresentFollowing,
					}
				case mmt.DescriptorTagMHLogoTransmission:
					if desc, err := mmt.ParseMHLogoTransmissionDescriptor(d); err == nil {
						logo = desc
					}
				}
			}
			if service == nil {
				continue
			}
			if logo != nil {
				ref := &model.LogoRef{LogoID: logo.LogoID}
				switch logo.TransmissionType {
				case mmt.LogoTransmissionTypeCDT1:
					version, downloadDataID := logo.LogoVersion, logo.DownloadDataID
					ref.Version, ref.DownloadDataID = &version, &downloadDataID
					direct[logo.LogoID] = logo
				case mmt.LogoTransmissionTypeCDT2:
					indirect[entry.ServiceID] = logo.LogoID
				case mmt.LogoTransmissionTypeSimple:
					ref.SimpleLogo, ref.HasSimpleLogo = logo.SimpleLogo, true
				}
				service.Logo = ref
			}
			services[entry.ServiceID] = service
		}
	}
	for serviceID, logoID := range indirect {
		logo, ok := direct[logoID]
		if !ok {
			continue
		}
		version, downloadDataID := logo.LogoVersion, logo.DownloadDataID
		services[serviceID].Logo.Version, services[serviceID].Logo.DownloadDataID = &version, &downloadDataID
	}
	s.services = services
	s.sdtReady = true
	s.applyRemoteControlKeys()
}

// applyRemoteControlKeys sets each service's one-touch button. Unlike
// ISDB-T/S, TLV-NIT assigns keys per service, not per transport stream.
func (s *serviceScan) applyRemoteControlKeys() {
	for serviceID, service := range s.services {
		service.RemoteControlKey = nil
		if key, ok := s.keys[serviceID]; ok {
			service.RemoteControlKey = &key
		}
	}
}

// Complete reports whether complete current MH-SDT[actual] and TLV-NIT
// tables arrived.
func (s *serviceScan) Complete() bool { return s.sdtReady && s.nitReady }

// Services returns the services in service ID order.
func (s *serviceScan) Services() []model.Service {
	ids := make([]int, 0, len(s.services))
	for id := range s.services {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	services := make([]model.Service, 0, len(ids))
	for _, id := range ids {
		services = append(services, *s.services[uint16(id)])
	}
	return services
}
