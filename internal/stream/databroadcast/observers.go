package databroadcast

import (
	"context"
	"encoding/hex"
	"slices"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

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
