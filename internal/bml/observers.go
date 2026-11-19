package bml

import (
	"bytes"
	"context"
	"encoding/hex"
	"slices"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

func (h *Hub) Observe(section ts.PIDSection) {
	h.ObserveContext(context.Background(), section)
}

// ObserveContext serializes section state transitions separately from readers.
func (h *Hub) ObserveContext(ctx context.Context, section ts.PIDSection) {
	h.observeMu.Lock()
	defer h.observeMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	switch section.Section.TableID() {
	case ts.TableIDPMT:
		h.observePMT(section)
	case ts.TableIDDSMCCDII:
		h.observeDII(ctx, section)
	case ts.TableIDDSMCCDDB:
		h.observeDDB(ctx, section)
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

func (h *Hub) ObservePCR(pid uint16, pcr PCR) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for serviceID, service := range h.services {
		if service.pmt == nil || service.pmt.PCRPID != pid {
			continue
		}
		service.pcr = &pcr
		h.broadcastLocked(serviceID, Event{Type: "pcr", PCR: clonePCR(&pcr)})
	}
}

func (h *Hub) observeBIT(section ts.Section) {
	bit, err := ts.ParseBIT(section)
	if err != nil || !bit.CurrentNext {
		return
	}
	value := &BIT{OriginalNetworkID: bit.OriginalNetworkID, Version: bit.VersionNumber, RawSectionHex: hex.EncodeToString(section)}
	for _, source := range bit.Broadcasters {
		b := Broadcaster{BroadcasterID: source.BroadcasterID, Services: []Service{}, Affiliations: []byte{}, AffiliationBroadcasters: []AffiliatedBroadcaster{}}
		for _, descriptor := range source.Descriptors {
			switch descriptor.Tag() {
			case ts.DescriptorTagBroadcasterName:
				if name, err := ts.ParseBroadcasterNameDescriptor(descriptor); err == nil {
					b.BroadcasterName = ptr(name)
				}
			case ts.DescriptorTagServiceList:
				if list, err := ts.ParseServiceListDescriptor(descriptor); err == nil {
					for _, service := range list.Services {
						b.Services = append(b.Services, Service{ServiceID: service.ServiceID, ServiceType: service.ServiceType})
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
					b.AffiliationBroadcasters = append(b.AffiliationBroadcasters, AffiliatedBroadcaster{OriginalNetworkID: affiliated.OriginalNetworkID, BroadcasterID: affiliated.BroadcasterID})
				}
			}
		}
		value.Broadcasters = append(value.Broadcasters, b)
	}
	h.mu.Lock()
	h.bit = value
	for serviceID := range h.subs {
		h.broadcastLocked(serviceID, Event{Type: "bit", BIT: cloneBIT(value)})
	}
	h.mu.Unlock()
}

func (h *Hub) observePMT(section ts.PIDSection) {
	pmt, err := ts.ParsePMT(section.Section)
	if err != nil {
		return
	}
	components := make([]Component, 0)
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
		component := Component{
			ComponentTag:   tag,
			PID:            elem.ElementaryPID,
			StreamType:     elem.StreamType,
			CarouselStatus: "waitingForDii",
		}
		if descriptor, ok := dataComponentDescriptor(elem.Descriptors); ok {
			component.DataComponentID = ptr(descriptor.DataComponentID)
			if isAribBXMLDataComponent(descriptor.DataComponentID) {
				parsed, _ := ts.ParseAdditionalAribBXMLInfo(descriptor.AdditionalDataComponentInfo)
				component.BXMLInfo = bxmlInfoFromTS(parsed)
			}
		}
		components = append(components, component)
	}
	slices.SortFunc(components, func(a, b Component) int {
		return int(a.ComponentTag) - int(b.ComponentTag)
	})
	h.mu.Lock()
	service := h.serviceLocked(pmt.ProgramNumber)
	pmtSection := string(section.Section)
	if service.pmtSection == pmtSection {
		h.mu.Unlock()
		return
	}
	// Reset a component whose ES changed, even if the tag was reused.
	if service.pmt != nil {
		for _, previous := range service.pmt.Components {
			if tag, ok := pidToTag[previous.PID]; !ok || tag != previous.ComponentTag {
				delete(service.carousels, previous.ComponentTag)
				delete(service.carouselStates, previous.ComponentTag)
				delete(service.diiSections, previous.ComponentTag)
			}
		}
	}
	service.pmtSection = pmtSection
	service.pmt = &PMT{
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
	event := Event{Type: "pmt", PMT: clonePMT(service.pmt)}
	h.broadcastLocked(pmt.ProgramNumber, event)
	h.mu.Unlock()
}

func (h *Hub) observeES(section ts.PIDSection) {
	stream, err := ts.ParseDSMCCStream(section.Section)
	if err != nil || !stream.CurrentNext {
		return
	}
	events := make([]GeneralEvent, 0, len(stream.Descriptors))
	for _, descriptor := range stream.Descriptors {
		if reference, ok := ts.ParseDSMCCNPTReference(descriptor); ok {
			events = append(events, GeneralEvent{Type: "nptReference", NPTReference: &NPTReference{PostDiscontinuityIndicator: reference.PostDiscontinuityIndicator, DSMContentID: reference.DSMContentID, STCReference: reference.STCReference, NPTReference: reference.NPTReference, ScaleNumerator: reference.ScaleNumerator, ScaleDenominator: reference.ScaleDenominator}})
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
		event := GeneralEvent{Type: eventType, EventMessageGroupID: item.EventMessageGroupID, TimeMode: item.TimeMode, TimeValueHex: hex.EncodeToString(item.TimeValue), EventMessageType: item.EventMessageType, EventMessageID: item.EventMessageID, PrivateData: item.PrivateData}
		if npt, ok := item.EventMessageNPT(); ok {
			event.EventMessageNPT = ptr(npt)
		}
		events = append(events, event)
	}
	h.mu.Lock()
	for _, ref := range h.carouselsByPIDLocked(section.PID) {
		e := &ESEvent{ComponentTag: ref.componentTag, DataEventID: stream.DataEventID, EventMessageGroupID: stream.EventMessageGroupID, Version: stream.VersionNumber, SectionNumber: stream.SectionNumber, RawSectionHex: hex.EncodeToString(section.Section), Events: events}
		h.broadcastLocked(ref.serviceID, Event{Type: "esEventUpdated", ESEvent: e})
	}
	h.mu.Unlock()
}

func (h *Hub) observeDII(ctx context.Context, section ts.PIDSection) {
	dii, err := ts.ParseDSMCCDII(section.Section)
	if err != nil {
		h.recordCarousel("dii", "invalid")
		return
	}
	raw := string(section.Section)
	h.mu.Lock()
	refs := h.carouselsByPIDLocked(section.PID)
	if len(refs) == 0 {
		h.mu.Unlock()
		h.recordCarousel("dii", "unmapped")
		return
	}
	// Merge peers starting this download together. A service appearing mid-
	// download keeps its own state until a later download boundary so it
	// cannot inherit a released payload stored only under a sibling's key.
	if len(refs) > 1 {
		canShare := true
		for _, ref := range refs {
			if state, ok := h.services[ref.serviceID].carouselStates[ref.componentTag]; ok && state.downloadID == dii.DownloadID {
				canShare = false
				break
			}
		}
		if canShare {
			for i := range refs {
				ref := &refs[i]
				ref.carousel = refs[0].carousel
				h.services[ref.serviceID].carousels[ref.componentTag] = ref.carousel
			}
		}
	}
	groups := groupCarousels(refs)
	h.mu.Unlock()
	for _, group := range groups {
		h.mu.Lock()
		ref := group[0]
		if h.services[ref.serviceID].diiSections[ref.componentTag] == raw {
			h.mu.Unlock()
			h.recordCarousel("dii", "duplicate")
			continue
		}
		ref.carousel.ObserveDII(dii)
		announcements := ref.carousel.AnnouncementMetadata()
		for _, peer := range group {
			h.announceDIILocked(peer, dii, raw, announcements)
		}
		h.mu.Unlock()
		h.recordCarousel("dii", "accepted")
		h.restoreModules(ctx, group, announcements)
	}
}

func (h *Hub) announceDIILocked(ref dataBroadcastCarouselRef, dii *ts.DSMCCDII, raw string, announcements []ts.DSMCCModuleAnnouncement) {
	serviceID, componentTag, carousel := ref.serviceID, ref.componentTag, ref.carousel
	service := h.services[serviceID]
	service.diiSections[componentTag] = raw
	rejections := carousel.RejectedAnnouncements()
	status := "active"
	if len(announcements) == 0 {
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
	modules := make([]Module, 0, len(announcements)+len(rejections))
	for _, announcement := range announcements {
		module := announcement.Module
		if !announcement.Complete {
			key := dataBroadcastModuleKey{componentTag: componentTag, downloadID: dii.DownloadID, moduleID: module.ModuleID, version: module.Version}
			service.moduleStarts[key] = now
		}
		modules = append(modules, announcedModule(componentTag, announcement))
	}

	for _, rejection := range rejections {
		modules = append(modules, rejectedModule(componentTag, dii.DownloadID, rejection))
	}
	event := Event{Type: "moduleListUpdated", ModuleList: &ModuleList{
		ComponentTag:  componentTag,
		DownloadID:    dii.DownloadID,
		BlockSize:     dii.BlockSize,
		DataEventID:   dataEventID,
		ReturnToEntry: returnToEntry,
		Modules:       modules,
	}}
	h.broadcastLocked(serviceID, event)
}

func (h *Hub) restoreModules(ctx context.Context, group []dataBroadcastCarouselRef, announcements []ts.DSMCCModuleAnnouncement) {
	if h.moduleStore == nil {
		return
	}
	carousel := group[0].carousel
	store, contextual := h.moduleStore.(ContextModuleStore)
	_, persistent := h.moduleStore.(PersistentModuleStore)
	for _, announcement := range announcements {
		if announcement.Complete {
			continue
		}
		info := announcement.Module
		downloadID := info.DownloadID
		var cached ts.DSMCCModule
		allMatch := true
		for i, peer := range group {
			if ctx.Err() != nil {
				return
			}
			key := h.moduleCacheKey(peer.serviceID, peer.componentTag, downloadID, info.ModuleID, info.Version, info.Size)
			var candidate ts.DSMCCModule
			var found bool
			if contextual {
				candidate, found = store.GetContext(ctx, key)
			} else {
				candidate, found = h.moduleStore.Get(key)
			}
			if !found || (i > 0 && (!bytes.Equal(cached.Data, candidate.Data) || !bytes.Equal(cached.Info, candidate.Info))) {
				allMatch = false
				break
			}
			cached = candidate
		}
		if !allMatch {
			h.recordCarousel("cache", "miss")
			continue
		}
		h.mu.Lock()
		restored := carousel.Restore(cached)
		if restored {
			if persistent {
				carousel.ReleaseCompletedPayload(info.ModuleID)
			}
			for _, ref := range group {
				delete(h.services[ref.serviceID].moduleStarts, dataBroadcastModuleKey{componentTag: ref.componentTag, downloadID: downloadID, moduleID: info.ModuleID, version: info.Version})
				h.broadcastLocked(ref.serviceID, Event{Type: "moduleUpdated", Module: ptr(apiModule(ref.componentTag, cached, false))})
			}
		}
		h.mu.Unlock()
		if restored {
			h.recordCarousel("cache", "hit")
		} else {
			h.recordCarousel("cache", "miss")
		}
	}
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

func (h *Hub) observeDDB(ctx context.Context, section ts.PIDSection) {
	ddb, err := ts.ParseDSMCCDDB(section.Section)
	if err != nil {
		h.recordCarousel("ddb", "invalid")
		return
	}
	h.mu.Lock()
	groups := groupCarousels(h.carouselsByPIDLocked(section.PID))
	h.mu.Unlock()
	if len(groups) == 0 {
		h.recordCarousel("ddb", "unmapped")
		return
	}
	for _, group := range groups {
		h.completeBlock(ctx, group, ddb)
	}
}

func (h *Hub) completeBlock(ctx context.Context, group []dataBroadcastCarouselRef, ddb *ts.DSMCCDDB) {
	carousel := group[0].carousel
	h.mu.Lock()
	module, complete, result, err := carousel.ObserveDDBWithResult(ddb)
	if !complete {
		h.mu.Unlock()
		if err != nil {
			result = "error"
		}
		h.recordCarousel("ddb", string(result))
		return
	}
	keys := make([]ModuleCacheKey, 0, len(group))
	for _, ref := range group {
		serviceID, componentTag := ref.serviceID, ref.componentTag
		key := dataBroadcastModuleKey{componentTag: componentTag, downloadID: module.DownloadID, moduleID: module.ModuleID, version: module.Version}
		service := h.services[serviceID]
		if started, ok := service.moduleStarts[key]; ok {
			observability.RecordDataBroadcastModuleDuration(ctx, h.channelType, h.channelID, time.Since(started).Milliseconds())
		}
		delete(service.moduleStarts, key)
		h.broadcastLocked(serviceID, Event{Type: "moduleUpdated", Module: ptr(apiModule(componentTag, *module, false))})
		keys = append(keys, h.moduleCacheKey(serviceID, componentTag, module.DownloadID, module.ModuleID, module.Version, module.Size))
	}
	h.mu.Unlock()
	h.recordCarousel("ddb", string(result))

	if h.moduleStore == nil || ctx.Err() != nil {
		return
	}
	var saved bool
	if store, ok := h.moduleStore.(ContextModuleStore); ok {
		saved = store.PutModules(ctx, keys, *module)
	} else {
		saved = true
		for _, key := range keys {
			if ctx.Err() != nil || !h.moduleStore.Put(key, *module) {
				saved = false
			}
		}
	}
	if _, persistent := h.moduleStore.(PersistentModuleStore); persistent && saved {
		// Section observations are serialized, and HTTP can only invalidate
		// released modules, so this completion is still the same generation.
		h.mu.Lock()
		carousel.ReleaseCompletedPayload(module.ModuleID)
		h.mu.Unlock()
	}
}

func (h *Hub) observeEIT(section ts.Section) {
	eit, err := ts.ParseEIT(section)
	if err != nil {
		return
	}
	eventIDs := make([]uint16, 0, len(eit.Events))
	for _, item := range eit.Events {
		eventIDs = append(eventIDs, item.EventID)
	}
	info := &ProgramInfo{
		ServiceID:     eit.ServiceID,
		EventIDs:      eventIDs,
		RawSectionHex: hex.EncodeToString(section),
	}
	h.mu.Lock()
	service := h.serviceLocked(eit.ServiceID)
	service.programInfo = info
	h.broadcastLocked(eit.ServiceID, Event{Type: "programInfo", ProgramInfo: cloneProgramInfo(info)})
	h.mu.Unlock()
}

func (h *Hub) observeTOT(section ts.Section) {
	tot, err := ts.ParseTOT(section)
	if err != nil {
		return
	}
	current := &CurrentTime{JSTTimeUnixMilli: tot.JSTTime.UnixMilli()}
	h.mu.Lock()
	for serviceID, service := range h.services {
		service.currentTime = current
		h.broadcastLocked(serviceID, Event{Type: "currentTime", CurrentTime: cloneCurrentTime(current)})
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

func componentTagExists(components []Component, tag byte) bool {
	for _, component := range components {
		if component.ComponentTag == tag {
			return true
		}
	}
	return false
}
