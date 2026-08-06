package databroadcast

import (
	"context"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

const dataBroadcastSubscriberBuffer = 64

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
	if ok {
		return apiModule(componentTag, cached, true), true
	}
	// This generation is still what the carousel announces as complete, but
	// its payload was released after a persistent Put and then evicted from
	// the store before this fetch. Reset assembly so the DDB blocks the
	// broadcaster keeps retransmitting rebuild it instead of leaving the
	// module permanently unfetchable.
	if carousel.Invalidate(moduleID, downloadID, version) {
		h.recordCarousel("module", "invalidated")
	}
	return DataBroadcastModule{}, false
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
	for _, ref := range h.carouselsByPIDLocked(section.PID) {
		info, ok := ref.carousel.ModuleInfo(ddb.ModuleID)
		if !ok || info.Version != ddb.ModuleVersion {
			continue
		}
		metadata, ok := info.Metadata()
		if !ok {
			continue
		}
		if metadata.CachingPriority != nil && *metadata.CachingPriority > priority {
			priority = *metadata.CachingPriority
		}
		name := strings.ToLower(path.Base(metadata.Name))
		if name == "index.bml" || name == "index.xhtml" {
			entryDocument = true
		}
	}
	return priority, entryDocument
}

// PersistableState returns the raw PMT and per-component DII sections needed
// to reconstruct a provisional snapshot, for every service that currently has
// a PMT. It is a cheap copy of bytes already retained for duplicate
// detection (service.pmtSection / service.diiSections), so it is safe to call
// periodically from a caller that owns persistence.
func (h *DataBroadcastHub) PersistableState() []PersistedService {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]PersistedService, 0, len(h.services))
	for serviceID, service := range h.services {
		if service.pmt == nil {
			continue
		}
		persisted := PersistedService{ServiceID: serviceID, PMTSection: []byte(service.pmtSection)}
		for _, component := range service.pmt.Components {
			diiSection, ok := service.diiSections[component.ComponentTag]
			if !ok {
				continue
			}
			persisted.Carousels = append(persisted.Carousels, PersistedCarousel{
				ComponentTag: component.ComponentTag,
				PID:          component.PID,
				DIISection:   []byte(diiSection),
			})
		}
		result = append(result, persisted)
	}
	return result
}

func (h *DataBroadcastHub) moduleCacheKey(serviceID uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte, size uint32) ModuleCacheKey {
	return ModuleCacheKey{ChannelType: h.channelType, ChannelID: h.channelID, ServiceID: serviceID, ComponentTag: componentTag, DownloadID: downloadID, ModuleID: moduleID, Version: version, Size: size}
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

// dataBroadcastCarouselRef identifies one service's view of a carousel PID.
type dataBroadcastCarouselRef struct {
	serviceID    uint16
	componentTag byte
	carousel     *ts.DSMCCCarousel
}

// carouselsByPIDLocked returns every service that maps pid to a data
// component. ISDB muxes routinely share one data-carousel ES between sibling
// services (e.g. TOKYO MX1/MX2 both carry component 0x60 on the same PID), so
// a section must be delivered to every referencing service: handing it to just
// one leaves the other services' carousels permanently missing that block.
// Results are ordered by serviceID so delivery is deterministic.
func (h *DataBroadcastHub) carouselsByPIDLocked(pid uint16) []dataBroadcastCarouselRef {
	refs := make([]dataBroadcastCarouselRef, 0, 2)
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
		refs = append(refs, dataBroadcastCarouselRef{serviceID: serviceID, componentTag: tag, carousel: carousel})
	}
	slices.SortFunc(refs, func(a, b dataBroadcastCarouselRef) int { return int(a.serviceID) - int(b.serviceID) })
	return refs
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
