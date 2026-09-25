package cache

import (
	"github.com/21S1298001/mahiron/internal/bml"
)

// RestoreSnapshot rebuilds a provisional snapshot from persisted PMT/DII
// sections. The canonical implementation lives in bml (it replays through
// the Hub); this wrapper keeps cache-package callers compiling.
func RestoreSnapshot(channelType, channelID string, persisted PersistedService, existence ModuleExistenceStore) bml.Snapshot {
	return bml.RestoreSnapshot(channelType, channelID, persisted, existence)
}
