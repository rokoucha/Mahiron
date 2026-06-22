package ts

import (
	"context"
	"errors"
	"io"
	"sort"
)

// ServiceInfo represents a scanned service in the scanner JSON output.
type ServiceInfo struct {
	Nid                uint16  `json:"nid"`
	Tsid               uint16  `json:"tsid"`
	Sid                uint16  `json:"sid"`
	Name               string  `json:"name"`
	Type               uint8   `json:"type"`
	LogoId             int64   `json:"logoId"`
	LogoVersion        *uint16 `json:"logoVersion,omitempty"`
	LogoDownloadDataId *uint16 `json:"logoDownloadDataId,omitempty"`
	RemoteControlKeyId *uint8  `json:"remoteControlKeyId,omitempty"`
}

// ServiceScanner reads a TS stream and outputs a list of services.
type ServiceScanner struct{}

// NewServiceScanner creates a new ServiceScanner.
func NewServiceScanner() *ServiceScanner {
	return &ServiceScanner{}
}

// Scan reads TS from src and returns detected services.
func (s *ServiceScanner) Scan(ctx context.Context, src io.Reader) ([]ServiceInfo, error) {
	return s.ScanServices(ctx, src)
}

// ScanServices reads TS from src and returns detected services.
func (s *ServiceScanner) ScanServices(ctx context.Context, src io.Reader) ([]ServiceInfo, error) {
	state := newServiceScanState()
	reader := NewPacketReader(src)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		packet, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return state.serviceList(), nil
			}
			return nil, err
		}
		if packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() {
			continue
		}
		if err := state.observe(packet); err != nil {
			return nil, err
		}
		if state.complete() {
			return state.serviceList(), nil
		}
	}
}

type serviceScanState struct {
	pat         *PAT
	nitSeen     bool
	assemblers  map[uint16]*SectionAssembler
	services    map[uint16]ServiceInfo
	remoteKeys  map[uint16]uint8
	sdtServices map[uint16]struct{}
}

func newServiceScanState() *serviceScanState {
	return &serviceScanState{
		assemblers:  map[uint16]*SectionAssembler{},
		services:    map[uint16]ServiceInfo{},
		remoteKeys:  map[uint16]uint8{},
		sdtServices: map[uint16]struct{}{},
	}
}

func (s *serviceScanState) observe(packet Packet) error {
	pid := packet.PID()
	if pid != PIDPAT && pid != PIDSDT && pid != PIDNIT {
		return nil
	}
	assembler := s.assemblers[pid]
	if assembler == nil {
		assembler = NewSectionAssembler(pid)
		s.assemblers[pid] = assembler
	}
	sections, err := assembler.FeedAll(packet)
	if err != nil {
		return err
	}
	for _, section := range sections {
		switch section.TableID() {
		case TableIDPAT:
			if pid != PIDPAT {
				continue
			}
			pat, err := ParsePAT(section)
			if err == nil {
				s.pat = pat
			}
		case TableIDSDT0, TableIDSDT1:
			if pid != PIDSDT {
				continue
			}
			s.handleSDT(section)
		case TableIDNIT0:
			if pid != PIDNIT {
				continue
			}
			s.handleNIT(section)
		}
	}
	return nil
}

func (s *serviceScanState) handleSDT(section Section) {
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
			Nid:    sdt.OriginalNetworkID,
			Tsid:   sdt.TransportStreamID,
			Sid:    svc.ServiceID,
			Name:   desc.ServiceName,
			Type:   desc.ServiceType,
			LogoId: -1,
		}
		if logo := LogoDescriptorFromDescriptors(svc.Descriptors); logo != nil {
			info.LogoId = int64(logo.LogoID)
			if logo.TransmissionType == LogoTransmissionTypeCDTDirect {
				info.LogoVersion = uint16Ptr(logo.LogoVersion)
				info.LogoDownloadDataId = uint16Ptr(logo.DownloadDataID)
			}
		}
		if key, ok := s.remoteKeys[sdt.TransportStreamID]; ok {
			info.RemoteControlKeyId = uint8Ptr(key)
		}
		s.services[svc.ServiceID] = info
		s.sdtServices[svc.ServiceID] = struct{}{}
	}
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

func (s *serviceScanState) handleNIT(section Section) {
	keys := remoteKeysFromNIT(section)
	if len(keys) == 0 && !section.ValidateCRC() {
		return
	}
	s.nitSeen = true
	for tsid, key := range keys {
		s.remoteKeys[tsid] = key
	}
	for sid, info := range s.services {
		key, ok := s.remoteKeys[info.Tsid]
		if !ok {
			continue
		}
		info.RemoteControlKeyId = uint8Ptr(key)
		s.services[sid] = info
	}
}

func remoteKeysFromNIT(section Section) map[uint16]uint8 {
	keys := map[uint16]uint8{}
	if len(section) < 12 || section.TableID() != TableIDNIT0 || section.TotalLength() > len(section) || !section.ValidateCRC() {
		return keys
	}
	sectionEnd := section.TotalLength() - 4
	networkDescriptorsLen := int(uint16(section[8]&0x0f)<<8 | uint16(section[9]))
	off := 10 + networkDescriptorsLen
	if off+2 > sectionEnd {
		return keys
	}
	transportStreamLoopLen := int(uint16(section[off]&0x0f)<<8 | uint16(section[off+1]))
	off += 2
	loopEnd := off + transportStreamLoopLen
	if loopEnd > sectionEnd {
		return keys
	}
	for off+6 <= loopEnd {
		tsid := uint16(section[off])<<8 | uint16(section[off+1])
		descriptorsLen := int(uint16(section[off+4]&0x0f)<<8 | uint16(section[off+5]))
		descStart := off + 6
		descEnd := descStart + descriptorsLen
		if descEnd > loopEnd {
			return keys
		}
		for _, desc := range ParseDescriptors(section[descStart:descEnd]) {
			if desc.Tag() == DescriptorTagTerrestrialDeliverySystem && len(desc.Data()) > 0 {
				keys[tsid] = desc.Data()[0]
			}
		}
		off = descEnd
	}
	return keys
}

func (s *serviceScanState) complete() bool {
	if s.pat == nil || !s.nitSeen {
		return false
	}
	for serviceID := range s.pat.Programs {
		if _, ok := s.sdtServices[serviceID]; !ok {
			return false
		}
	}
	return true
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
