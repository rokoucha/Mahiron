package databroadcast

import (
	"fmt"

	"github.com/21S1298001/mahiron/ts"
)

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
