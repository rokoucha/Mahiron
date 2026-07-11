package databroadcast

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"

	"github.com/21S1298001/mahiron/ts"
)

const dataBroadcastSubscriberBuffer = 64

type DataBroadcastEvent struct {
	Type        string
	Snapshot    DataBroadcastSnapshot
	PMT         *DataBroadcastPMT
	ModuleList  *DataBroadcastModuleList
	Module      *DataBroadcastModule
	ProgramInfo *DataBroadcastProgramInfo
	CurrentTime *DataBroadcastCurrentTime
	ESEvent     *DataBroadcastESEvent
	BIT         *DataBroadcastBIT
	PCR         *DataBroadcastPCR
}

type DataBroadcastSnapshot struct {
	ServiceID   uint16
	PMT         *DataBroadcastPMT
	Components  []DataBroadcastComponent
	ProgramInfo *DataBroadcastProgramInfo
	CurrentTime *DataBroadcastCurrentTime
	BIT         *DataBroadcastBIT
	PCR         *DataBroadcastPCR
}

type DataBroadcastPMT struct {
	ServiceID     uint16
	Version       byte
	PCRPID        uint16
	Components    []DataBroadcastComponent
	RawSectionHex string
}

type DataBroadcastComponent struct {
	ComponentTag byte
	PID          uint16
	StreamType   byte
	Modules      []DataBroadcastModule
}

type DataBroadcastModuleList struct {
	ComponentTag byte
	DownloadID   uint32
	BlockSize    uint16
	Modules      []DataBroadcastModule
}

type DataBroadcastModule struct {
	ComponentTag byte
	ModuleID     uint16
	DownloadID   uint32
	Version      byte
	Size         uint32
	Info         []byte
	Complete     bool
	ETag         string
	Data         []byte
}

type DataBroadcastProgramInfo struct {
	ServiceID     uint16
	EventIDs      []uint16
	RawSectionHex string
}

type DataBroadcastCurrentTime struct {
	JSTTimeUnixMilli int64
}

type DataBroadcastESEvent struct {
	ComponentTag        byte
	DataEventID         byte
	EventMessageGroupID uint16
	Version             byte
	SectionNumber       byte
	Events              []DataBroadcastGeneralEvent
	RawSectionHex       string
}

type DataBroadcastGeneralEvent struct {
	Type                string
	EventMessageGroupID uint16
	TimeMode            byte
	TimeValueHex        string
	EventMessageType    byte
	EventMessageID      uint16
	PrivateData         []byte
	EventMessageNPT     *uint64
	NPTReference        *DataBroadcastNPTReference
}

type DataBroadcastNPTReference struct {
	PostDiscontinuityIndicator bool
	DSMContentID               byte
	STCReference               uint64
	NPTReference               uint64
	ScaleNumerator             int16
	ScaleDenominator           int16
}

type DataBroadcastPCR struct {
	PCRBase      uint64
	PCRExtension uint16
}

type DataBroadcastBIT struct {
	OriginalNetworkID uint16
	Version           byte
	Broadcasters      []DataBroadcastBroadcaster
	RawSectionHex     string
}

type DataBroadcastBroadcaster struct {
	BroadcasterID            byte
	BroadcasterName          *string
	Services                 []DataBroadcastService
	Affiliations             []byte
	AffiliationBroadcasters  []DataBroadcastAffiliatedBroadcaster
	TerrestrialBroadcasterID *uint16
}

type DataBroadcastService struct {
	ServiceID   uint16
	ServiceType byte
}
type DataBroadcastAffiliatedBroadcaster struct {
	OriginalNetworkID uint16
	BroadcasterID     byte
}

type DataBroadcastHub struct {
	mu       sync.Mutex
	services map[uint16]*dataBroadcastService
	subs     map[uint16]map[chan DataBroadcastEvent]struct{}
	bit      *DataBroadcastBIT
}

type dataBroadcastService struct {
	pmt         *DataBroadcastPMT
	pidToTag    map[uint16]byte
	carousels   map[byte]*ts.DSMCCCarousel
	programInfo *DataBroadcastProgramInfo
	currentTime *DataBroadcastCurrentTime
	pcr         *DataBroadcastPCR
}

func NewDataBroadcastHub() *DataBroadcastHub {
	return &DataBroadcastHub{
		services: map[uint16]*dataBroadcastService{},
		subs:     map[uint16]map[chan DataBroadcastEvent]struct{}{},
	}
}

func (h *DataBroadcastHub) Subscribe(ctx context.Context, serviceID uint16) (DataBroadcastSnapshot, <-chan DataBroadcastEvent, func()) {
	ch := make(chan DataBroadcastEvent, dataBroadcastSubscriberBuffer)
	h.mu.Lock()
	snapshot := h.snapshotLocked(serviceID)
	if h.subs[serviceID] == nil {
		h.subs[serviceID] = map[chan DataBroadcastEvent]struct{}{}
	}
	h.subs[serviceID][ch] = struct{}{}
	h.mu.Unlock()
	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs[serviceID], ch)
			if len(h.subs[serviceID]) == 0 {
				delete(h.subs, serviceID)
			}
			h.mu.Unlock()
			close(ch)
		})
	}
	go func() {
		<-ctx.Done()
		unsubscribe()
	}()
	return snapshot, ch, unsubscribe
}

func (h *DataBroadcastHub) Observe(section ts.PIDSection) {
	switch section.Section.TableID() {
	case ts.TableIDPMT:
		h.observePMT(section)
	case ts.TableIDDSMCCDII:
		h.observeDII(section)
	case ts.TableIDDSMCCDDB:
		h.observeDDB(section)
	case ts.TableIDDSMCCStream:
		h.observeES(section)
	case ts.TableIDBIT:
		h.observeBIT(section.Section)
	case ts.TableIDEITPF0, ts.TableIDEITPF1:
		h.observeEIT(section.Section)
	case ts.TableIDTOT:
		h.observeTOT(section.Section)
	}
}

func (h *DataBroadcastHub) ObservePacket(packet ts.Packet) {
	base, extension, ok := packet.ProgramClockReference()
	if !ok {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for serviceID, service := range h.services {
		if service.pmt == nil || service.pmt.PCRPID != packet.PID() {
			continue
		}
		pcr := &DataBroadcastPCR{PCRBase: base, PCRExtension: extension}
		service.pcr = pcr
		h.broadcastLocked(serviceID, DataBroadcastEvent{Type: "pcr", PCR: clonePCR(pcr)})
	}
}

func (h *DataBroadcastHub) observeBIT(section ts.Section) {
	bit, err := ts.ParseBIT(section)
	if err != nil || !bit.CurrentNext {
		return
	}
	value := &DataBroadcastBIT{OriginalNetworkID: bit.OriginalNetworkID, Version: bit.VersionNumber, RawSectionHex: hex.EncodeToString(section)}
	for _, source := range bit.Broadcasters {
		b := DataBroadcastBroadcaster{BroadcasterID: source.BroadcasterID, Services: []DataBroadcastService{}, Affiliations: []byte{}, AffiliationBroadcasters: []DataBroadcastAffiliatedBroadcaster{}}
		for _, descriptor := range source.Descriptors {
			switch descriptor.Tag() {
			case ts.DescriptorTagBroadcasterName:
				if name, err := ts.ParseBroadcasterNameDescriptor(descriptor); err == nil {
					b.BroadcasterName = ptr(name)
				}
			case ts.DescriptorTagServiceList:
				if list, err := ts.ParseServiceListDescriptor(descriptor); err == nil {
					for _, service := range list.Services {
						b.Services = append(b.Services, DataBroadcastService{ServiceID: service.ServiceID, ServiceType: service.ServiceType})
					}
				}
			case ts.DescriptorTagExtendedBroadcaster:
				extended, err := ts.ParseExtendedBroadcasterDescriptor(descriptor)
				if err != nil {
					continue
				}
				b.Affiliations = append([]byte(nil), extended.AffiliationIDs...)
				if extended.BroadcasterType == 1 || extended.BroadcasterType == 2 {
					b.TerrestrialBroadcasterID = ptr(extended.TerrestrialBroadcasterID)
				}
				for _, affiliated := range extended.Broadcasters {
					b.AffiliationBroadcasters = append(b.AffiliationBroadcasters, DataBroadcastAffiliatedBroadcaster{OriginalNetworkID: affiliated.OriginalNetworkID, BroadcasterID: affiliated.BroadcasterID})
				}
			}
		}
		value.Broadcasters = append(value.Broadcasters, b)
	}
	h.mu.Lock()
	h.bit = value
	for serviceID := range h.subs {
		h.broadcastLocked(serviceID, DataBroadcastEvent{Type: "bit", BIT: cloneBIT(value)})
	}
	h.mu.Unlock()
}

func (h *DataBroadcastHub) Module(serviceID uint16, componentTag byte, moduleID uint16) (DataBroadcastModule, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	service := h.services[serviceID]
	if service == nil {
		return DataBroadcastModule{}, false
	}
	carousel := service.carousels[componentTag]
	if carousel == nil {
		return DataBroadcastModule{}, false
	}
	module, ok := carousel.Module(moduleID)
	if !ok {
		return DataBroadcastModule{}, false
	}
	return apiModule(componentTag, module, true), true
}

func (h *DataBroadcastHub) observePMT(section ts.PIDSection) {
	pmt, err := ts.ParsePMT(section.Section)
	if err != nil {
		return
	}
	components := make([]DataBroadcastComponent, 0)
	pidToTag := map[uint16]byte{}
	for _, elem := range pmt.Elements {
		if elem.StreamType != ts.StreamTypeDSMCCUNMessages && elem.StreamType != ts.StreamTypeDSMCCDataCarousel && elem.StreamType != ts.StreamTypeDSMCCStreamDescriptors {
			continue
		}
		tag, ok := streamIdentifierComponentTag(elem.Descriptors)
		if !ok {
			continue
		}
		pidToTag[elem.ElementaryPID] = tag
		components = append(components, DataBroadcastComponent{
			ComponentTag: tag,
			PID:          elem.ElementaryPID,
			StreamType:   elem.StreamType,
		})
	}
	slices.SortFunc(components, func(a, b DataBroadcastComponent) int {
		return int(a.ComponentTag) - int(b.ComponentTag)
	})
	h.mu.Lock()
	service := h.serviceLocked(pmt.ProgramNumber)
	service.pmt = &DataBroadcastPMT{
		ServiceID:     pmt.ProgramNumber,
		Version:       pmt.VersionNumber,
		PCRPID:        pmt.PCRPID,
		Components:    components,
		RawSectionHex: hex.EncodeToString(section.Section),
	}
	service.pidToTag = pidToTag
	if service.carousels == nil {
		service.carousels = map[byte]*ts.DSMCCCarousel{}
	}
	for tag := range service.carousels {
		if !componentTagExists(components, tag) {
			delete(service.carousels, tag)
		}
	}
	for _, component := range components {
		if service.carousels[component.ComponentTag] == nil {
			service.carousels[component.ComponentTag] = ts.NewDSMCCCarousel(ts.DSMCCCarouselLimits{})
		}
	}
	event := DataBroadcastEvent{Type: "pmt", PMT: clonePMT(service.pmt)}
	h.broadcastLocked(pmt.ProgramNumber, event)
	h.mu.Unlock()
}

func (h *DataBroadcastHub) observeES(section ts.PIDSection) {
	stream, err := ts.ParseDSMCCStream(section.Section)
	if err != nil || !stream.CurrentNext {
		return
	}
	h.mu.Lock()
	serviceID, componentTag, _, ok := h.carouselByPIDLocked(section.PID)
	if !ok {
		h.mu.Unlock()
		return
	}
	e := &DataBroadcastESEvent{ComponentTag: componentTag, DataEventID: stream.DataEventID, EventMessageGroupID: stream.EventMessageGroupID, Version: stream.VersionNumber, SectionNumber: stream.SectionNumber, RawSectionHex: hex.EncodeToString(section.Section)}
	for _, descriptor := range stream.Descriptors {
		if reference, ok := ts.ParseDSMCCNPTReference(descriptor); ok {
			e.Events = append(e.Events, DataBroadcastGeneralEvent{Type: "nptReference", NPTReference: &DataBroadcastNPTReference{PostDiscontinuityIndicator: reference.PostDiscontinuityIndicator, DSMContentID: reference.DSMContentID, STCReference: reference.STCReference, NPTReference: reference.NPTReference, ScaleNumerator: reference.ScaleNumerator, ScaleDenominator: reference.ScaleDenominator}})
			continue
		}
		item, ok := ts.ParseDSMCCGeneralEvent(descriptor)
		if !ok {
			continue
		}
		eventType := "event"
		switch item.TimeMode {
		case 0:
			eventType = "immediateEvent"
		case 2:
			eventType = "nptEvent"
		}
		event := DataBroadcastGeneralEvent{Type: eventType, EventMessageGroupID: item.EventMessageGroupID, TimeMode: item.TimeMode, TimeValueHex: hex.EncodeToString(item.TimeValue), EventMessageType: item.EventMessageType, EventMessageID: item.EventMessageID, PrivateData: item.PrivateData}
		if npt, ok := item.EventMessageNPT(); ok {
			event.EventMessageNPT = ptr(npt)
		}
		e.Events = append(e.Events, event)
	}
	h.broadcastLocked(serviceID, DataBroadcastEvent{Type: "esEventUpdated", ESEvent: e})
	h.mu.Unlock()
}

func (h *DataBroadcastHub) observeDII(section ts.PIDSection) {
	dii, err := ts.ParseDSMCCDII(section.Section)
	if err != nil {
		return
	}
	h.mu.Lock()
	serviceID, componentTag, carousel, ok := h.carouselByPIDLocked(section.PID)
	if !ok {
		h.mu.Unlock()
		return
	}
	infos := carousel.ObserveDII(dii)
	modules := make([]DataBroadcastModule, 0, len(infos))
	for _, info := range infos {
		modules = append(modules, DataBroadcastModule{
			ComponentTag: componentTag,
			ModuleID:     info.ModuleID,
			DownloadID:   dii.DownloadID,
			Version:      info.Version,
			Size:         info.ModuleSize,
			Info:         append([]byte(nil), info.Info...),
			ETag:         moduleETag(dii.DownloadID, info.ModuleID, info.Version, info.ModuleSize),
		})
	}
	event := DataBroadcastEvent{Type: "moduleListUpdated", ModuleList: &DataBroadcastModuleList{
		ComponentTag: componentTag,
		DownloadID:   dii.DownloadID,
		BlockSize:    dii.BlockSize,
		Modules:      modules,
	}}
	h.broadcastLocked(serviceID, event)
	h.mu.Unlock()
}

func (h *DataBroadcastHub) observeDDB(section ts.PIDSection) {
	ddb, err := ts.ParseDSMCCDDB(section.Section)
	if err != nil {
		return
	}
	h.mu.Lock()
	serviceID, componentTag, carousel, ok := h.carouselByPIDLocked(section.PID)
	if !ok {
		h.mu.Unlock()
		return
	}
	module, complete, err := carousel.ObserveDDB(ddb)
	if err != nil || !complete {
		h.mu.Unlock()
		return
	}
	event := DataBroadcastEvent{Type: "moduleUpdated", Module: ptr(apiModule(componentTag, *module, false))}
	h.broadcastLocked(serviceID, event)
	h.mu.Unlock()
}

func (h *DataBroadcastHub) observeEIT(section ts.Section) {
	eit, err := ts.ParseEIT(section)
	if err != nil {
		return
	}
	eventIDs := make([]uint16, 0, len(eit.Events))
	for _, item := range eit.Events {
		eventIDs = append(eventIDs, item.EventID)
	}
	info := &DataBroadcastProgramInfo{
		ServiceID:     eit.ServiceID,
		EventIDs:      eventIDs,
		RawSectionHex: hex.EncodeToString(section),
	}
	h.mu.Lock()
	service := h.serviceLocked(eit.ServiceID)
	service.programInfo = info
	h.broadcastLocked(eit.ServiceID, DataBroadcastEvent{Type: "programInfo", ProgramInfo: cloneProgramInfo(info)})
	h.mu.Unlock()
}

func (h *DataBroadcastHub) observeTOT(section ts.Section) {
	tot, err := ts.ParseTOT(section)
	if err != nil {
		return
	}
	current := &DataBroadcastCurrentTime{JSTTimeUnixMilli: tot.JSTTime.UnixMilli()}
	h.mu.Lock()
	for serviceID, service := range h.services {
		service.currentTime = current
		h.broadcastLocked(serviceID, DataBroadcastEvent{Type: "currentTime", CurrentTime: cloneCurrentTime(current)})
	}
	h.mu.Unlock()
}

func (h *DataBroadcastHub) serviceLocked(serviceID uint16) *dataBroadcastService {
	service := h.services[serviceID]
	if service == nil {
		service = &dataBroadcastService{
			pidToTag:  map[uint16]byte{},
			carousels: map[byte]*ts.DSMCCCarousel{},
		}
		h.services[serviceID] = service
	}
	return service
}

func (h *DataBroadcastHub) carouselByPIDLocked(pid uint16) (uint16, byte, *ts.DSMCCCarousel, bool) {
	for serviceID, service := range h.services {
		tag, ok := service.pidToTag[pid]
		if !ok {
			continue
		}
		carousel := service.carousels[tag]
		if carousel == nil {
			carousel = ts.NewDSMCCCarousel(ts.DSMCCCarouselLimits{})
			service.carousels[tag] = carousel
		}
		return serviceID, tag, carousel, true
	}
	return 0, 0, nil, false
}

func (h *DataBroadcastHub) snapshotLocked(serviceID uint16) DataBroadcastSnapshot {
	service := h.services[serviceID]
	snapshot := DataBroadcastSnapshot{ServiceID: serviceID, BIT: cloneBIT(h.bit)}
	if service == nil {
		return snapshot
	}
	snapshot.PMT = clonePMT(service.pmt)
	snapshot.ProgramInfo = cloneProgramInfo(service.programInfo)
	snapshot.CurrentTime = cloneCurrentTime(service.currentTime)
	snapshot.PCR = clonePCR(service.pcr)
	if service.pmt != nil {
		snapshot.Components = cloneComponents(service.pmt.Components)
		for i := range snapshot.Components {
			carousel := service.carousels[snapshot.Components[i].ComponentTag]
			if carousel == nil {
				continue
			}
			for _, module := range carousel.Modules() {
				snapshot.Components[i].Modules = append(snapshot.Components[i].Modules, apiModule(snapshot.Components[i].ComponentTag, module, false))
			}
		}
	}
	return snapshot
}

func (h *DataBroadcastHub) broadcastLocked(serviceID uint16, event DataBroadcastEvent) {
	for ch := range h.subs[serviceID] {
		select {
		case ch <- event:
		default:
		}
	}
}

func streamIdentifierComponentTag(descriptors []ts.Descriptor) (byte, bool) {
	for _, desc := range descriptors {
		if desc.Tag() == 0x52 && desc.Length() >= 1 {
			return desc.Data()[0], true
		}
	}
	return 0, false
}

func componentTagExists(components []DataBroadcastComponent, tag byte) bool {
	for _, component := range components {
		if component.ComponentTag == tag {
			return true
		}
	}
	return false
}

func apiModule(componentTag byte, module ts.DSMCCModule, includeData bool) DataBroadcastModule {
	result := DataBroadcastModule{
		ComponentTag: componentTag,
		ModuleID:     module.ModuleID,
		DownloadID:   module.DownloadID,
		Version:      module.Version,
		Size:         module.Size,
		Info:         append([]byte(nil), module.Info...),
		Complete:     true,
		ETag:         moduleETag(module.DownloadID, module.ModuleID, module.Version, module.Size),
	}
	if includeData {
		result.Data = append([]byte(nil), module.Data...)
	}
	return result
}

func moduleETag(downloadID uint32, moduleID uint16, version byte, size uint32) string {
	return fmt.Sprintf(`"dsmcc-%08x-%04x-%02x-%08x"`, downloadID, moduleID, version, size)
}

func clonePMT(pmt *DataBroadcastPMT) *DataBroadcastPMT {
	if pmt == nil {
		return nil
	}
	clone := *pmt
	clone.Components = cloneComponents(pmt.Components)
	return &clone
}

func cloneComponents(components []DataBroadcastComponent) []DataBroadcastComponent {
	result := make([]DataBroadcastComponent, len(components))
	for i, component := range components {
		result[i] = component
		result[i].Modules = cloneModules(component.Modules)
	}
	return result
}

func cloneModules(modules []DataBroadcastModule) []DataBroadcastModule {
	result := make([]DataBroadcastModule, len(modules))
	for i, module := range modules {
		result[i] = module
		result[i].Info = append([]byte(nil), module.Info...)
		result[i].Data = append([]byte(nil), module.Data...)
	}
	return result
}

func cloneProgramInfo(info *DataBroadcastProgramInfo) *DataBroadcastProgramInfo {
	if info == nil {
		return nil
	}
	clone := *info
	clone.EventIDs = append([]uint16(nil), info.EventIDs...)
	return &clone
}

func cloneCurrentTime(current *DataBroadcastCurrentTime) *DataBroadcastCurrentTime {
	if current == nil {
		return nil
	}
	clone := *current
	return &clone
}

func cloneBIT(bit *DataBroadcastBIT) *DataBroadcastBIT {
	if bit == nil {
		return nil
	}
	clone := *bit
	clone.Broadcasters = make([]DataBroadcastBroadcaster, len(bit.Broadcasters))
	for i, b := range bit.Broadcasters {
		clone.Broadcasters[i] = b
		clone.Broadcasters[i].Services = append([]DataBroadcastService(nil), b.Services...)
		clone.Broadcasters[i].Affiliations = append([]byte(nil), b.Affiliations...)
		clone.Broadcasters[i].AffiliationBroadcasters = append([]DataBroadcastAffiliatedBroadcaster(nil), b.AffiliationBroadcasters...)
		if b.BroadcasterName != nil {
			clone.Broadcasters[i].BroadcasterName = ptr(*b.BroadcasterName)
		}
		if b.TerrestrialBroadcasterID != nil {
			clone.Broadcasters[i].TerrestrialBroadcasterID = ptr(*b.TerrestrialBroadcasterID)
		}
	}
	return &clone
}

func clonePCR(pcr *DataBroadcastPCR) *DataBroadcastPCR {
	if pcr == nil {
		return nil
	}
	clone := *pcr
	return &clone
}

func ptr[T any](v T) *T {
	return &v
}
