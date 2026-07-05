package ts

import (
	"errors"
	"slices"
)

const (
	DefaultDSMCCMaxModuleSize               = 1040896
	DefaultDSMCCMaxCompletedBytesPerService = 64 * 1024 * 1024
	DefaultDSMCCMaxInFlightBytesPerService  = 16 * 1024 * 1024
)

var ErrDSMCCCarouselBudgetExceeded = errors.New("ts: dsm-cc carousel budget exceeded")

type DSMCCCarouselLimits struct {
	MaxModuleSize     uint32
	MaxCompletedBytes uint64
	MaxInFlightBytes  uint64
}

func (l DSMCCCarouselLimits) withDefaults() DSMCCCarouselLimits {
	if l.MaxModuleSize == 0 {
		l.MaxModuleSize = DefaultDSMCCMaxModuleSize
	}
	if l.MaxCompletedBytes == 0 {
		l.MaxCompletedBytes = DefaultDSMCCMaxCompletedBytesPerService
	}
	if l.MaxInFlightBytes == 0 {
		l.MaxInFlightBytes = DefaultDSMCCMaxInFlightBytesPerService
	}
	return l
}

type DSMCCModule struct {
	DownloadID uint32
	ModuleID   uint16
	Version    byte
	Size       uint32
	Info       []byte
	Data       []byte
	Generation uint64
}

type DSMCCCarousel struct {
	limits         DSMCCCarouselLimits
	modules        map[uint16]*dsmccModuleState
	completedBytes uint64
	inFlightBytes  uint64
	generation     uint64
}

type dsmccModuleState struct {
	info       DSMCCModuleInfo
	downloadID uint32
	blockSize  uint16
	data       []byte
	received   []bool
	count      int
	completed  bool
	generation uint64
}

func NewDSMCCCarousel(limits DSMCCCarouselLimits) *DSMCCCarousel {
	return &DSMCCCarousel{
		limits:  limits.withDefaults(),
		modules: map[uint16]*dsmccModuleState{},
	}
}

func (c *DSMCCCarousel) ObserveDII(dii *DSMCCDII) []DSMCCModuleInfo {
	if c.modules == nil {
		c.modules = map[uint16]*dsmccModuleState{}
	}
	seen := map[uint16]bool{}
	accepted := make([]DSMCCModuleInfo, 0, len(dii.Modules))
	for _, module := range dii.Modules {
		seen[module.ModuleID] = true
		if module.ModuleSize == 0 || module.ModuleSize > c.limits.MaxModuleSize || dii.BlockSize == 0 {
			c.remove(module.ModuleID)
			continue
		}
		accepted = append(accepted, module)
		current := c.modules[module.ModuleID]
		if current != nil && current.downloadID == dii.DownloadID && current.info.Version == module.Version && current.info.ModuleSize == module.ModuleSize && current.blockSize == dii.BlockSize {
			continue
		}
		c.remove(module.ModuleID)
		if uint64(module.ModuleSize) > c.limits.MaxInFlightBytes {
			continue
		}
		for c.inFlightBytes+uint64(module.ModuleSize) > c.limits.MaxInFlightBytes && c.evictOldestInFlight() {
		}
		if c.inFlightBytes+uint64(module.ModuleSize) > c.limits.MaxInFlightBytes {
			continue
		}
		blockCount := int((module.ModuleSize + uint32(dii.BlockSize) - 1) / uint32(dii.BlockSize))
		c.generation++
		c.modules[module.ModuleID] = &dsmccModuleState{
			info:       cloneDSMCCModuleInfo(module),
			downloadID: dii.DownloadID,
			blockSize:  dii.BlockSize,
			data:       make([]byte, module.ModuleSize),
			received:   make([]bool, blockCount),
			generation: c.generation,
		}
		c.inFlightBytes += uint64(module.ModuleSize)
	}
	for moduleID := range c.modules {
		if !seen[moduleID] {
			c.remove(moduleID)
		}
	}
	return accepted
}

func (c *DSMCCCarousel) ObserveDDB(ddb *DSMCCDDB) (*DSMCCModule, bool, error) {
	state := c.modules[ddb.ModuleID]
	if state == nil || state.completed || state.downloadID != ddb.DownloadID || state.info.Version != ddb.ModuleVersion || state.blockSize == 0 {
		return nil, false, nil
	}
	blockNumber := int(ddb.BlockNumber)
	if blockNumber >= len(state.received) {
		return nil, false, nil
	}
	off := blockNumber * int(state.blockSize)
	if off >= len(state.data) {
		return nil, false, nil
	}
	end := off + len(ddb.Data)
	if end > len(state.data) {
		end = len(state.data)
	}
	if !state.received[blockNumber] {
		state.received[blockNumber] = true
		state.count++
	}
	copy(state.data[off:end], ddb.Data[:end-off])
	if state.count < len(state.received) {
		return nil, false, nil
	}
	state.completed = true
	state.received = nil
	c.inFlightBytes -= uint64(state.info.ModuleSize)
	c.completedBytes += uint64(state.info.ModuleSize)
	for c.completedBytes > c.limits.MaxCompletedBytes && c.evictOldestCompletedExcept(ddb.ModuleID) {
	}
	if c.completedBytes > c.limits.MaxCompletedBytes {
		c.remove(ddb.ModuleID)
		return nil, false, ErrDSMCCCarouselBudgetExceeded
	}
	module := state.module()
	return &module, true, nil
}

func (c *DSMCCCarousel) Module(moduleID uint16) (DSMCCModule, bool) {
	state := c.modules[moduleID]
	if state == nil || !state.completed {
		return DSMCCModule{}, false
	}
	return state.module(), true
}

func (c *DSMCCCarousel) Modules() []DSMCCModule {
	result := make([]DSMCCModule, 0, len(c.modules))
	for _, state := range c.modules {
		if state.completed {
			result = append(result, state.module())
		}
	}
	slices.SortFunc(result, func(a, b DSMCCModule) int {
		return int(a.ModuleID) - int(b.ModuleID)
	})
	return result
}

func (c *DSMCCCarousel) CompletedBytes() uint64 {
	return c.completedBytes
}

func (c *DSMCCCarousel) InFlightBytes() uint64 {
	return c.inFlightBytes
}

func (c *DSMCCCarousel) remove(moduleID uint16) {
	state := c.modules[moduleID]
	if state == nil {
		return
	}
	if state.completed {
		c.completedBytes -= uint64(state.info.ModuleSize)
	} else {
		c.inFlightBytes -= uint64(state.info.ModuleSize)
	}
	delete(c.modules, moduleID)
}

func (c *DSMCCCarousel) evictOldestInFlight() bool {
	var oldestID uint16
	var oldest *dsmccModuleState
	for moduleID, state := range c.modules {
		if state.completed {
			continue
		}
		if oldest == nil || state.generation < oldest.generation {
			oldestID = moduleID
			oldest = state
		}
	}
	if oldest == nil {
		return false
	}
	c.remove(oldestID)
	return true
}

func (c *DSMCCCarousel) evictOldestCompletedExcept(except uint16) bool {
	var oldestID uint16
	var oldest *dsmccModuleState
	for moduleID, state := range c.modules {
		if !state.completed || moduleID == except {
			continue
		}
		if oldest == nil || state.generation < oldest.generation {
			oldestID = moduleID
			oldest = state
		}
	}
	if oldest == nil {
		return false
	}
	c.remove(oldestID)
	return true
}

func (s *dsmccModuleState) module() DSMCCModule {
	return DSMCCModule{
		DownloadID: s.downloadID,
		ModuleID:   s.info.ModuleID,
		Version:    s.info.Version,
		Size:       s.info.ModuleSize,
		Info:       append([]byte(nil), s.info.Info...),
		Data:       append([]byte(nil), s.data...),
		Generation: s.generation,
	}
}

func cloneDSMCCModuleInfo(module DSMCCModuleInfo) DSMCCModuleInfo {
	module.Info = append([]byte(nil), module.Info...)
	return module
}
