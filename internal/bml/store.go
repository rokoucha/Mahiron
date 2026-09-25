package bml

import (
	"github.com/21S1298001/mahiron/ts"
)

type ModuleCacheKey struct {
	ChannelType  string
	ChannelID    string
	ServiceID    uint16
	ComponentTag byte
	DownloadID   uint32
	ModuleID     uint16
	Version      byte
	Size         uint32
}

// ModuleVersionKey is the immutable URL identity of a module. Size remains a
// part of the DII identity used while restoring a live carousel, but is not
// present in resource URLs, so retained generations are looked up by this key.
type ModuleVersionKey struct {
	ChannelType  string
	ChannelID    string
	ServiceID    uint16
	ComponentTag byte
	DownloadID   uint32
	ModuleID     uint16
	Version      byte
}

func (k ModuleCacheKey) VersionKey() ModuleVersionKey {
	return ModuleVersionKey{
		ChannelType: k.ChannelType, ChannelID: k.ChannelID, ServiceID: k.ServiceID,
		ComponentTag: k.ComponentTag, DownloadID: k.DownloadID, ModuleID: k.ModuleID, Version: k.Version,
	}
}

// ModuleStore keeps completed modules across channel-session lifetimes. The
// assembler remains memory bounded; only completed, validated modules enter a
// store. The in-memory implementation lives in bml/cache.
type ModuleStore interface {
	Get(ModuleCacheKey) (ts.DSMCCModule, bool)
	GetVersion(ModuleVersionKey) (ts.DSMCCModule, bool)
	Put(ModuleCacheKey, ts.DSMCCModule) bool
}

// PersistentModuleStore keeps successfully written modules independently of
// the live carousel, allowing its completed payload buffer to be released.
type PersistentModuleStore interface {
	ModuleStore
	PersistsCompletedModules()
}

// EvictedModuleStore records immutable module identities that were once
// completed but were removed to satisfy its cache limit.
type EvictedModuleStore interface {
	WasEvicted(ModuleVersionKey) bool
}

// ModuleExistenceStore reports whether a completed module is retained without
// reading its payload. RestoreSnapshot uses this to mark modules complete in a
// provisional snapshot without paying for a full module read per module.
type ModuleExistenceStore interface {
	Has(ModuleCacheKey) bool
}

// PersistedCarousel is one component's raw DII section as observed live. It
// carries just enough identity to replay through Hub.Observe and reconstruct
// carousel state without touching any module payload bytes.
type PersistedCarousel struct {
	ComponentTag byte
	PID          uint16
	DIISection   []byte
}

// PersistedService is the raw PMT and per-component DII sections needed to
// reconstruct a provisional snapshot for one service via RestoreSnapshot.
type PersistedService struct {
	ServiceID  uint16
	PMTSection []byte
	Carousels  []PersistedCarousel
	// StoredAt is the unix time (seconds) the snapshot was last written. It is
	// populated by GetSnapshot and ignored by PutSnapshot, whose store
	// implementation stamps its own write time.
	StoredAt int64
}

// SnapshotStore persists the raw sections needed to reconstruct a provisional
// data-broadcast snapshot before a tuner has been acquired for a channel.
// Unlike ModuleStore, entries are not counted against a byte budget; a store
// implementation prunes them by age only.
type SnapshotStore interface {
	PutSnapshot(channelType, channelID string, service PersistedService) error
	GetSnapshot(channelType, channelID string, serviceID uint16) (PersistedService, bool)
}

// RestoreSnapshot rebuilds a provisional snapshot from persisted PMT/DII
// sections by replaying them through a throwaway hub, the same code path a
// live session uses to build carousel and component state. Module completion
// is derived from existence in the store, not from reading payload bytes, so
// this never pays for a full module read.
//
// The result deliberately omits programInfo, currentTime, and pcr: those are
// clock/schedule samples, not carousel state, and a stale value would be
// actively misleading rather than merely outdated.
func RestoreSnapshot(channelType, channelID string, persisted PersistedService, existence ModuleExistenceStore) Snapshot {
	hub := NewHub().WithMetricLabels(channelType, channelID)
	if len(persisted.PMTSection) > 0 {
		hub.Observe(ts.PIDSection{Section: ts.Section(persisted.PMTSection)})
		for _, carousel := range persisted.Carousels {
			if len(carousel.DIISection) > 0 {
				hub.Observe(ts.PIDSection{PID: carousel.PID, Section: ts.Section(carousel.DIISection)})
			}
		}
	}
	snapshot := hub.Snapshot(persisted.ServiceID)
	snapshot.ProgramInfo = nil
	snapshot.CurrentTime = nil
	snapshot.PCR = nil
	if existence == nil {
		return snapshot
	}
	for i := range snapshot.Components {
		componentTag := snapshot.Components[i].ComponentTag
		for j := range snapshot.Components[i].Modules {
			module := &snapshot.Components[i].Modules[j]
			if module.Status == "rejected" {
				continue
			}
			key := ModuleCacheKey{
				ChannelType: channelType, ChannelID: channelID, ServiceID: persisted.ServiceID,
				ComponentTag: componentTag, DownloadID: module.DownloadID, ModuleID: module.ModuleID,
				Version: module.Version, Size: module.Size,
			}
			if existence.Has(key) {
				module.Complete = true
				module.Status = "complete"
				module.ReceivedBlocks = module.TotalBlocks
			}
		}
	}
	return snapshot
}
