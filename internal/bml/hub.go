package bml

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/ts"
)

const dataBroadcastSubscriberBuffer = 64

type Hub struct {
	observeMu   sync.Mutex
	mu          sync.Mutex
	services    map[uint16]*dataBroadcastService
	subs        map[uint16]map[chan Event]*dataBroadcastSubscriber
	bit         *BIT
	channelType string
	channelID   string
	moduleStore ModuleStore
}

type dataBroadcastService struct {
	pmt            *PMT
	pmtSection     string
	pidToTag       map[uint16]byte
	carousels      map[byte]*ts.DSMCCCarousel
	diiSections    map[byte]string
	moduleStarts   map[dataBroadcastModuleKey]time.Time
	carouselStates map[byte]dataBroadcastCarouselState
	programInfo    *ProgramInfo
	currentTime    *CurrentTime
	pcr            *PCR
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

func NewHub() *Hub {
	return &Hub{
		services: map[uint16]*dataBroadcastService{},
		subs:     map[uint16]map[chan Event]*dataBroadcastSubscriber{},
	}
}

func (h *Hub) WithMetricLabels(channelType, channelID string) *Hub {
	h.channelType = channelType
	h.channelID = channelID
	return h
}

func (h *Hub) WithModuleStore(store ModuleStore) *Hub {
	h.moduleStore = store
	return h
}

func (h *Hub) recordCarousel(operation, result string) {
	observability.RecordDataBroadcastCarouselEvent(context.Background(), h.channelType, h.channelID, operation, result)
}

func (h *Hub) Subscribe(ctx context.Context, serviceID uint16) (Snapshot, <-chan Event, func()) {
	ch := make(chan Event, dataBroadcastSubscriberBuffer)
	h.mu.Lock()
	snapshot := h.snapshotLocked(serviceID)
	if h.subs[serviceID] == nil {
		h.subs[serviceID] = map[chan Event]*dataBroadcastSubscriber{}
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

func (h *Hub) Module(serviceID uint16, componentTag byte, moduleID uint16) (Module, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	service := h.services[serviceID]
	if service == nil {
		return Module{}, false
	}
	carousel := service.carousels[componentTag]
	if carousel == nil {
		return Module{}, false
	}
	module, ok := carousel.Module(moduleID)
	if !ok {
		return Module{}, false
	}
	return apiModule(componentTag, module, true), true
}

// Snapshot returns one self-consistent view of the current carousel state.
// Callers use it to reconcile after reconnecting an event stream.
func (h *Hub) Snapshot(serviceID uint16) Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotLocked(serviceID)
}

// ModuleVersion returns a completed module for the requested immutable URL.
// A currently announced generation is served only from its live carousel;
// previously announced generations may be served from the completed-module
// store. Thus an incomplete replacement never falls back to stale data, while
// an in-flight fetch for an already replaced generation remains valid.
func (h *Hub) ModuleVersion(serviceID uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte) (Module, bool) {
	h.mu.Lock()
	service := h.services[serviceID]
	var carousel *ts.DSMCCCarousel
	if service != nil {
		carousel = service.carousels[componentTag]
	}
	if carousel == nil {
		h.mu.Unlock()
		return Module{}, false
	}
	module, ok := carousel.Module(moduleID)
	generation, released := carousel.ReleasedGeneration(moduleID, downloadID, version)
	h.mu.Unlock()

	if ok && module.DownloadID == downloadID && module.Version == version {
		return apiModule(componentTag, module, true), true
	}
	if h.moduleStore == nil {
		return Module{}, false
	}
	key := h.moduleCacheKey(serviceID, componentTag, downloadID, moduleID, version, 0).VersionKey()
	cached, ok := h.moduleStore.GetVersion(key)
	if ok {
		return apiModule(componentTag, cached, true), true
	}

	// A cache miss obtained during a write must not invalidate a newer
	// completion or a carousel that has since changed PIDs.
	h.mu.Lock()
	if service.carousels[componentTag] == carousel {
		module, ok = carousel.Module(moduleID)
		if ok && module.DownloadID == downloadID && module.Version == version {
			h.mu.Unlock()
			return apiModule(componentTag, module, true), true
		}
		if current, stillReleased := carousel.ReleasedGeneration(moduleID, downloadID, version); released && stillReleased && generation == current {
			carousel.Invalidate(moduleID, downloadID, version)
			h.mu.Unlock()
			h.recordCarousel("module", "invalidated")
			return Module{}, false
		}
	}
	h.mu.Unlock()
	return Module{}, false
}

// PersistableState returns the raw PMT and per-component DII sections needed
// to reconstruct a provisional snapshot, for every service that currently has
// a PMT. It is a cheap copy of bytes already retained for duplicate
// detection (service.pmtSection / service.diiSections), so it is safe to call
// periodically from a caller that owns persistence.
func (h *Hub) PersistableState() []PersistedService {
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

func (h *Hub) moduleCacheKey(serviceID uint16, componentTag byte, downloadID uint32, moduleID uint16, version byte, size uint32) ModuleCacheKey {
	return ModuleCacheKey{ChannelType: h.channelType, ChannelID: h.channelID, ServiceID: serviceID, ComponentTag: componentTag, DownloadID: downloadID, ModuleID: moduleID, Version: version, Size: size}
}

func (h *Hub) serviceLocked(serviceID uint16) *dataBroadcastService {
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

// carouselsByPIDLocked returns the services referencing a data ES, including
// sibling services that share assembly. Results are ordered by service ID.
func (h *Hub) carouselsByPIDLocked(pid uint16) []dataBroadcastCarouselRef {
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

// groupCarousels preserves service ID order and coalesces refs sharing assembly.
func groupCarousels(refs []dataBroadcastCarouselRef) [][]dataBroadcastCarouselRef {
	if len(refs) == 0 {
		return nil
	}
	if !slices.ContainsFunc(refs[1:], func(ref dataBroadcastCarouselRef) bool {
		return ref.carousel != refs[0].carousel
	}) {
		return [][]dataBroadcastCarouselRef{refs}
	}
	var groups [][]dataBroadcastCarouselRef
	index := make(map[*ts.DSMCCCarousel]int)
	for _, ref := range refs {
		i, ok := index[ref.carousel]
		if !ok {
			i = len(groups)
			index[ref.carousel] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], ref)
	}
	return groups
}

func (h *Hub) broadcastLocked(serviceID uint16, event Event) {
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
