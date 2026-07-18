package databroadcast

import (
	"context"
	"encoding/hex"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

const dataBroadcastSubscriberBuffer = 64

type DataBroadcastEvent struct {
	Type string
	// Sequence orders notifications in one SSE connection. It is not a state
	// version: PCR notifications, for example, do not change the snapshot.
	Sequence    uint64
	Revision    uint64
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
	Revision    uint64
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
	ComponentTag       byte
	PID                uint16
	StreamType         byte
	DataComponentID    *uint16
	BXMLInfo           *ts.AdditionalAribBXMLInfo
	DataEventID        byte
	ReturnToEntry      *bool
	CarouselStatus     string
	CarouselDownloadID *uint32
	CarouselBlockSize  *uint16
	Modules            []DataBroadcastModule
}

type DataBroadcastModuleList struct {
	ComponentTag  byte
	DownloadID    uint32
	BlockSize     uint16
	DataEventID   byte
	ReturnToEntry *bool
	Modules       []DataBroadcastModule
}

type DataBroadcastModule struct {
	ComponentTag    byte
	ModuleID        uint16
	DownloadID      uint32
	Version         byte
	Size            uint32
	Info            []byte
	Complete        bool
	Status          string
	RejectionReason *string
	ReceivedBlocks  int
	TotalBlocks     int
	ETag            string
	Data            []byte
	Metadata        *ts.DSMCCModuleMetadata
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
	mu          sync.Mutex
	services    map[uint16]*dataBroadcastService
	subs        map[uint16]map[chan DataBroadcastEvent]*dataBroadcastSubscriber
	bit         *DataBroadcastBIT
	channelType string
	channelID   string
	moduleStore ModuleStore
}

type dataBroadcastService struct {
	pmt            *DataBroadcastPMT
	pmtSection     string
	pidToTag       map[uint16]byte
	carousels      map[byte]*ts.DSMCCCarousel
	diiSections    map[byte]string
	moduleStarts   map[dataBroadcastModuleKey]time.Time
	carouselStates map[byte]dataBroadcastCarouselState
	programInfo    *DataBroadcastProgramInfo
	currentTime    *DataBroadcastCurrentTime
	pcr            *DataBroadcastPCR
	revision       uint64
	sequence       uint64
}

type dataBroadcastCarouselState struct {
	status     string
	downloadID uint32
	blockSize  uint16
}

type dataBroadcastSubscriber struct {
	closed bool
}

type dataBroadcastModuleKey struct {
	componentTag byte
	downloadID   uint32
	moduleID     uint16
	version      byte
}

func NewDataBroadcastHub() *DataBroadcastHub {
	return &DataBroadcastHub{
		services: map[uint16]*dataBroadcastService{},
		subs:     map[uint16]map[chan DataBroadcastEvent]*dataBroadcastSubscriber{},
	}
}

func (h *DataBroadcastHub) WithMetricLabels(channelType, channelID string) *DataBroadcastHub {
	h.channelType = channelType
	h.channelID = channelID
	return h
}

func (h *DataBroadcastHub) WithModuleCache(cache *ModuleCache) *DataBroadcastHub {
	return h.WithModuleStore(cache)

}

func (h *DataBroadcastHub) WithModuleStore(store ModuleStore) *DataBroadcastHub {
	h.moduleStore = store
	return h
}

func (h *DataBroadcastHub) recordCarousel(operation, result string) {
	observability.RecordDataBroadcastCarouselEvent(context.Background(), h.channelType, h.channelID, operation, result)
}

func (h *DataBroadcastHub) Subscribe(ctx context.Context, serviceID uint16) (DataBroadcastSnapshot, <-chan DataBroadcastEvent, func()) {
	ch := make(chan DataBroadcastEvent, dataBroadcastSubscriberBuffer)
	h.mu.Lock()
	snapshot := h.snapshotLocked(serviceID)
	if h.subs[serviceID] == nil {
		h.subs[serviceID] = map[chan DataBroadcastEvent]*dataBroadcastSubscriber{}
	}
	h.subs[serviceID][ch] = &dataBroadcastSubscriber{}
	h.mu.Unlock()
	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			h.mu.Lock()
			subscriber, ok := h.subs[serviceID][ch]
			if !ok {
				h.mu.Unlock()
				return
			}
			delete(h.subs[serviceID], ch)
			if len(h.subs[serviceID]) == 0 {
				delete(h.subs, serviceID)
			}
			h.mu.Unlock()
			if !subscriber.closed {
				close(ch)
			}
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

// Snapshot returns one self-consistent view of the current carousel state.
// Callers use it to reconcile after reconnecting an event stream.
func (h *DataBroadcastHub) Snapshot(serviceID uint16) DataBroadcastSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotLocked(serviceID)
}

// ModuleVersion returns a completed module for the requested immutable URL.
// A currently announced generation is served only from its live carousel;
// previously announced generations may be served from the completed-module
// store. Thus an incomplete replacement never falls back to stale data, while
// an in-flight fetch for an already replaced generation remains valid.
func (h *DataBroadcastHub) ModuleVersion(serviceID uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte) (DataBroadcastModule, bool) {
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
	if ok && module.DownloadID == downloadID && module.Version == version {
		return apiModule(componentTag, module, true), true
	}
	if h.moduleStore == nil {
		return DataBroadcastModule{}, false
	}
	cached, ok := h.moduleStore.GetVersion(h.moduleCacheKey(serviceID, componentTag, downloadID, moduleID, version, 0).VersionKey())
	if !ok {
		return DataBroadcastModule{}, false
	}
	return apiModule(componentTag, cached, true), true
}

// DDBPriority returns the cache priority announced by DII and whether the
// block belongs to the BML entry document. It is intentionally read-only and
// used by the channel session before placing DDB work on a bounded queue.
func (h *DataBroadcastHub) DDBPriority(section ts.PIDSection) (priority byte, entryDocument bool) {
	ddb, err := ts.ParseDSMCCDDB(section.Section)
	if err != nil {
		return 0, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	_, _, carousel, ok := h.carouselByPIDLocked(section.PID)
	if !ok {
		return 0, false
	}
	info, ok := carousel.ModuleInfo(ddb.ModuleID)
	if !ok || info.Version != ddb.ModuleVersion {
		return 0, false
	}
	metadata, ok := info.Metadata()
	if !ok {
		return 0, false
	}
	if metadata.CachingPriority != nil {
		priority = *metadata.CachingPriority
	}
	name := strings.ToLower(path.Base(metadata.Name))
	return priority, name == "index.bml" || name == "index.xhtml"
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
		component := DataBroadcastComponent{
			ComponentTag:   tag,
			PID:            elem.ElementaryPID,
			StreamType:     elem.StreamType,
			CarouselStatus: "waitingForDii",
		}
		if descriptor, ok := dataComponentDescriptor(elem.Descriptors); ok {
			component.DataComponentID = ptr(descriptor.DataComponentID)
			if isAribBXMLDataComponent(descriptor.DataComponentID) {
				component.BXMLInfo, _ = ts.ParseAdditionalAribBXMLInfo(descriptor.AdditionalDataComponentInfo)
			}
		}
		components = append(components, component)
	}
	slices.SortFunc(components, func(a, b DataBroadcastComponent) int {
		return int(a.ComponentTag) - int(b.ComponentTag)
	})
	h.mu.Lock()
	service := h.serviceLocked(pmt.ProgramNumber)
	pmtSection := string(section.Section)
	if service.pmtSection == pmtSection {
		h.mu.Unlock()
		return
	}
	service.pmtSection = pmtSection
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
			delete(service.carouselStates, tag)
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
		h.recordCarousel("dii", "invalid")
		return
	}
	h.mu.Lock()
	serviceID, componentTag, carousel, ok := h.carouselByPIDLocked(section.PID)
	if !ok {
		h.mu.Unlock()
		h.recordCarousel("dii", "unmapped")
		return
	}
	service := h.services[serviceID]
	diiSection := string(section.Section)
	if service.diiSections[componentTag] == diiSection {
		h.mu.Unlock()
		h.recordCarousel("dii", "duplicate")
		return
	}
	service.diiSections[componentTag] = diiSection
	infos := carousel.ObserveDII(dii)
	rejections := carousel.RejectedAnnouncements()
	status := "active"
	if len(infos) == 0 {
		status = "empty"
	}
	for _, rejection := range rejections {
		if rejection.Reason == "moduleSizeLimitExceeded" || rejection.Reason == "inFlightBudgetExceeded" {
			status = "resourceLimitExceeded"
		} else if status != "resourceLimitExceeded" {
			status = "unsupported"
		}
	}
	service.carouselStates[componentTag] = dataBroadcastCarouselState{status: status, downloadID: dii.DownloadID, blockSize: dii.BlockSize}
	dataEventID := byte(dii.DownloadID >> 28)
	returnToEntry := diiReturnToEntry(dii.PrivateData)
	if service.pmt != nil {
		for i := range service.pmt.Components {
			if service.pmt.Components[i].ComponentTag == componentTag {
				service.pmt.Components[i].DataEventID = dataEventID
				service.pmt.Components[i].ReturnToEntry = returnToEntry
				service.pmt.Components[i].CarouselStatus = status
				service.pmt.Components[i].CarouselDownloadID = ptr(dii.DownloadID)
				service.pmt.Components[i].CarouselBlockSize = ptr(dii.BlockSize)
				break
			}
		}
	}
	now := time.Now()
	for key := range service.moduleStarts {
		if key.componentTag == componentTag {
			delete(service.moduleStarts, key)
		}
	}
	modules := make([]DataBroadcastModule, 0, len(infos)+len(rejections))
	restored := make([]DataBroadcastModule, 0)
	for _, info := range infos {
		key := dataBroadcastModuleKey{componentTag: componentTag, downloadID: dii.DownloadID, moduleID: info.ModuleID, version: info.Version}
		service.moduleStarts[key] = now
		module := DataBroadcastModule{
			ComponentTag: componentTag,
			ModuleID:     info.ModuleID,
			DownloadID:   dii.DownloadID,
			Version:      info.Version,
			Size:         info.ModuleSize,
			Info:         append([]byte(nil), info.Info...),
			ETag:         moduleETag(dii.DownloadID, info.ModuleID, info.Version, info.ModuleSize),
			Status:       "announced",
			TotalBlocks:  int((info.ModuleSize + uint32(dii.BlockSize) - 1) / uint32(dii.BlockSize)),
		}
		if metadata, ok := info.Metadata(); ok {
			module.Metadata = &metadata
		}
		modules = append(modules, module)
		cacheKey := h.moduleCacheKey(serviceID, componentTag, dii.DownloadID, info.ModuleID, info.Version, info.ModuleSize)
		if h.moduleStore != nil {
			cached, found := h.moduleStore.Get(cacheKey)
			if found && carousel.Restore(cached) {
				modules[len(modules)-1].Complete = true
				modules[len(modules)-1].Status = "complete"
				modules[len(modules)-1].ReceivedBlocks = modules[len(modules)-1].TotalBlocks
				restored = append(restored, apiModule(componentTag, cached, false))
				delete(service.moduleStarts, dataBroadcastModuleKey{componentTag: componentTag, downloadID: dii.DownloadID, moduleID: info.ModuleID, version: info.Version})
				h.recordCarousel("cache", "hit")
				if _, persistent := h.moduleStore.(PersistentModuleStore); persistent {
					carousel.ReleaseCompletedPayload(info.ModuleID)
				}
			} else {
				h.recordCarousel("cache", "miss")
			}
		}
	}
	for _, rejection := range rejections {
		modules = append(modules, rejectedModule(componentTag, dii.DownloadID, rejection))
	}
	event := DataBroadcastEvent{Type: "moduleListUpdated", ModuleList: &DataBroadcastModuleList{
		ComponentTag:  componentTag,
		DownloadID:    dii.DownloadID,
		BlockSize:     dii.BlockSize,
		DataEventID:   dataEventID,
		ReturnToEntry: returnToEntry,
		Modules:       modules,
	}}
	h.broadcastLocked(serviceID, event)
	for i := range restored {
		h.broadcastLocked(serviceID, DataBroadcastEvent{Type: "moduleUpdated", Module: &restored[i]})
	}
	h.mu.Unlock()
	h.recordCarousel("dii", "accepted")
}

func diiReturnToEntry(privateData []byte) *bool {
	for len(privateData) >= 2 {
		tag, length := privateData[0], int(privateData[1])
		privateData = privateData[2:]
		if length > len(privateData) {
			return nil
		}
		if tag == 0xf0 && length > 0 {
			value := privateData[0]&0x80 != 0
			return &value
		}
		privateData = privateData[length:]
	}
	return nil
}

func (h *DataBroadcastHub) observeDDB(section ts.PIDSection) {
	ddb, err := ts.ParseDSMCCDDB(section.Section)
	if err != nil {
		h.recordCarousel("ddb", "invalid")
		return
	}
	h.mu.Lock()
	serviceID, componentTag, carousel, ok := h.carouselByPIDLocked(section.PID)
	if !ok {
		h.mu.Unlock()
		h.recordCarousel("ddb", "unmapped")
		return
	}
	module, complete, result, err := carousel.ObserveDDBWithResult(ddb)
	if err != nil || !complete {
		h.mu.Unlock()
		if err != nil {
			h.recordCarousel("ddb", "error")
		} else {
			h.recordCarousel("ddb", string(result))
		}
		return
	}
	key := dataBroadcastModuleKey{componentTag: componentTag, downloadID: module.DownloadID, moduleID: module.ModuleID, version: module.Version}
	started, timed := h.services[serviceID].moduleStarts[key]
	delete(h.services[serviceID].moduleStarts, key)
	event := DataBroadcastEvent{Type: "moduleUpdated", Module: ptr(apiModule(componentTag, *module, false))}
	if h.moduleStore != nil && h.moduleStore.Put(h.moduleCacheKey(serviceID, componentTag, module.DownloadID, module.ModuleID, module.Version, module.Size), *module) {
		if _, persistent := h.moduleStore.(PersistentModuleStore); persistent {
			carousel.ReleaseCompletedPayload(module.ModuleID)
		}
	}
	h.broadcastLocked(serviceID, event)
	h.mu.Unlock()
	h.recordCarousel("ddb", "completed")
	if timed {
		observability.RecordDataBroadcastModuleDuration(context.Background(), h.channelType, h.channelID, time.Since(started).Milliseconds())
	}
}

func (h *DataBroadcastHub) moduleCacheKey(serviceID uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte, size uint32) ModuleCacheKey {
	return ModuleCacheKey{ChannelType: h.channelType, ChannelID: h.channelID, ServiceID: serviceID, ComponentTag: componentTag, DownloadID: downloadID, ModuleID: moduleID, Version: version, Size: size}
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
			pidToTag:       map[uint16]byte{},
			carousels:      map[byte]*ts.DSMCCCarousel{},
			diiSections:    map[byte]string{},
			moduleStarts:   map[dataBroadcastModuleKey]time.Time{},
			carouselStates: map[byte]dataBroadcastCarouselState{},
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
	snapshot.Revision = service.revision
	snapshot.PMT = clonePMT(service.pmt)
	snapshot.ProgramInfo = cloneProgramInfo(service.programInfo)
	snapshot.CurrentTime = cloneCurrentTime(service.currentTime)
	snapshot.PCR = clonePCR(service.pcr)
	if service.pmt != nil {
		snapshot.Components = cloneComponents(service.pmt.Components)
		for i := range snapshot.Components {
			if state, ok := service.carouselStates[snapshot.Components[i].ComponentTag]; ok {
				snapshot.Components[i].CarouselStatus = state.status
				snapshot.Components[i].CarouselDownloadID = ptr(state.downloadID)
				snapshot.Components[i].CarouselBlockSize = ptr(state.blockSize)
			}
			carousel := service.carousels[snapshot.Components[i].ComponentTag]
			if carousel == nil {
				continue
			}
			for _, announcement := range carousel.Announcements() {
				module := apiModule(snapshot.Components[i].ComponentTag, announcement.Module, false)
				module.Complete = announcement.Complete
				module.ReceivedBlocks = announcement.ReceivedBlocks
				module.TotalBlocks = announcement.TotalBlocks
				if announcement.Complete {
					module.Status = "complete"
				} else if announcement.ReceivedBlocks > 0 {
					module.Status = "receiving"
				} else {
					module.Status = "announced"
				}
				snapshot.Components[i].Modules = append(snapshot.Components[i].Modules, module)
			}
			for _, rejection := range carousel.RejectedAnnouncements() {
				downloadID := uint32(0)
				if snapshot.Components[i].CarouselDownloadID != nil {
					downloadID = *snapshot.Components[i].CarouselDownloadID
				}
				snapshot.Components[i].Modules = append(snapshot.Components[i].Modules, rejectedModule(snapshot.Components[i].ComponentTag, downloadID, rejection))
			}
		}
	}
	return snapshot
}

func rejectedModule(componentTag byte, downloadID uint32, rejection ts.DSMCCModuleRejection) DataBroadcastModule {
	reason := rejection.Reason
	module := DataBroadcastModule{
		ComponentTag: componentTag, ModuleID: rejection.Module.ModuleID,
		DownloadID: downloadID, Version: rejection.Module.Version, Size: rejection.Module.ModuleSize,
		Info: append([]byte(nil), rejection.Module.Info...), Status: "rejected", RejectionReason: &reason,
		ETag: moduleETag(downloadID, rejection.Module.ModuleID, rejection.Module.Version, rejection.Module.ModuleSize),
	}
	if metadata, ok := rejection.Module.Metadata(); ok {
		module.Metadata = &metadata
	}
	return module
}

func (h *DataBroadcastHub) broadcastLocked(serviceID uint16, event DataBroadcastEvent) {
	service := h.serviceLocked(serviceID)
	// PCR is a clock sample, not a material carousel state change.
	if event.Type != "pcr" {
		service.revision++
	}
	service.sequence++
	event.Revision = service.revision
	event.Sequence = service.sequence
	for ch, subscriber := range h.subs[serviceID] {
		if subscriber.closed {
			continue
		}
		select {
		case ch <- event:
		default:
			// Deltas are no longer reliable. Closing lets EventSource reconnect;
			// every connection starts with an authoritative snapshot.
			subscriber.closed = true
			delete(h.subs[serviceID], ch)
			close(ch)
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

func dataComponentDescriptor(descriptors []ts.Descriptor) (*ts.DataComponentDescriptor, bool) {
	for _, desc := range descriptors {
		if desc.Tag() != ts.DescriptorTagDataComponent {
			continue
		}
		value, err := ts.ParseDataComponentDescriptor(desc)
		return value, err == nil
	}
	return nil, false
}

func isAribBXMLDataComponent(id uint16) bool {
	return id == 0x0007 || id == 0x000b || id == 0x000c || id == 0x000d
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
		Status:       "complete",
		ETag:         moduleETag(module.DownloadID, module.ModuleID, module.Version, module.Size),
	}
	if metadata, ok := (ts.DSMCCModuleInfo{Info: module.Info}).Metadata(); ok {
		result.Metadata = &metadata
	}
	if includeData {
		result.Data = append([]byte(nil), module.Data...)
	}
	return result
}

// CompletedModule exposes a cached completed DSM-CC module using the same
// representation as live carousel modules.
func CompletedModule(componentTag byte, module ts.DSMCCModule) DataBroadcastModule {
	return apiModule(componentTag, module, true)
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
		if component.DataComponentID != nil {
			value := *component.DataComponentID
			result[i].DataComponentID = &value
		}
		result[i].BXMLInfo = cloneBXMLInfo(component.BXMLInfo)
		result[i].Modules = cloneModules(component.Modules)
	}
	return result
}

func cloneBXMLInfo(info *ts.AdditionalAribBXMLInfo) *ts.AdditionalAribBXMLInfo {
	if info == nil {
		return nil
	}
	clone := *info
	if info.EntryPointInfo != nil {
		entry := *info.EntryPointInfo
		if entry.BXMLMajorVersion != nil {
			value := *entry.BXMLMajorVersion
			entry.BXMLMajorVersion = &value
		}
		if entry.BXMLMinorVersion != nil {
			value := *entry.BXMLMinorVersion
			entry.BXMLMinorVersion = &value
		}
		clone.EntryPointInfo = &entry
	}
	if info.AdditionalAribCarouselInfo != nil {
		carousel := *info.AdditionalAribCarouselInfo
		clone.AdditionalAribCarouselInfo = &carousel
	}
	return &clone
}

func cloneModules(modules []DataBroadcastModule) []DataBroadcastModule {
	result := make([]DataBroadcastModule, len(modules))
	for i, module := range modules {
		result[i] = module
		result[i].Info = append([]byte(nil), module.Info...)
		result[i].Data = append([]byte(nil), module.Data...)
		if module.Metadata != nil {
			metadata := *module.Metadata
			metadata.ExpireData = append([]byte(nil), metadata.ExpireData...)
			metadata.ActivationData = append([]byte(nil), metadata.ActivationData...)
			result[i].Metadata = &metadata
		}
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
