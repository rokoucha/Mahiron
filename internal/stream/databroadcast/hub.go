package databroadcast

import (
	"context"
	"path"
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
