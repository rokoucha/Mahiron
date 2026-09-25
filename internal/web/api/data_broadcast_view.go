package api

import (
	"fmt"
	"io"

	"github.com/go-faster/jx"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/bml/resource"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func writeDataBroadcastSSE(w io.Writer, serviceItemID int64, event bml.Event) error {
	e := &jx.Encoder{}
	encodeDataBroadcastEvent(e, apiDataBroadcastEvent(serviceItemID, event))
	if _, err := fmt.Fprintf(w, "event: %s\n", event.Type); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\n", event.Sequence); err != nil {
		return err
	}
	if _, err := w.Write([]byte("data: ")); err != nil {
		return err
	}
	if _, err := w.Write(e.Bytes()); err != nil {
		return err
	}
	_, err := w.Write([]byte("\n\n"))
	return err
}

// writeDataBroadcastJSON writes a data-broadcast response body with its
// generated encoder.
func writeDataBroadcastJSON(w io.Writer, value interface{ Encode(*jx.Encoder) }) error {
	e := &jx.Encoder{}
	value.Encode(e)
	_, err := w.Write(append(e.Bytes(), '\n'))
	return err
}

// apiDataBroadcastEvent converts a notification. Only the field named by the
// event type is set, and it is null when the notification carries nothing.
func apiDataBroadcastEvent(serviceItemID int64, event bml.Event) apigen.DataBroadcastEvent {
	result := apigen.DataBroadcastEvent{Type: event.Type, Sequence: int64(event.Sequence), Revision: int64(event.Revision)}
	switch event.Type {
	case "snapshot":
		result.Snapshot = apigen.NewOptDataBroadcastSnapshot(apiDataBroadcastSnapshot(serviceItemID, event.Snapshot, apigen.DataBroadcastSnapshotOriginLive, nil))
	case "pmt":
		result.Pmt = apigen.OptNilDataBroadcastPMT{Set: true, Null: event.PMT == nil}
		if event.PMT != nil {
			result.Pmt.Value = apiDataBroadcastPMT(serviceItemID, event.PMT)
		}
	case "moduleListUpdated":
		result.ModuleList = apigen.OptNilDataBroadcastModuleList{Set: true, Null: event.ModuleList == nil}
		if event.ModuleList != nil {
			result.ModuleList.Value = apiDataBroadcastModuleList(serviceItemID, event.ModuleList)
		}
	case "moduleUpdated":
		result.Module = apigen.OptNilDataBroadcastModule{Set: true, Null: event.Module == nil}
		if event.Module != nil {
			result.Module.Value = apiDataBroadcastModule(serviceItemID, event.Module)
		}
	case "programInfo":
		result.ProgramInfo = apigen.OptNilDataBroadcastProgramInfo{Set: true, Null: event.ProgramInfo == nil}
		if event.ProgramInfo != nil {
			result.ProgramInfo.Value = apiDataBroadcastProgramInfo(event.ProgramInfo)
		}
	case "currentTime":
		result.CurrentTime = apigen.OptNilDataBroadcastCurrentTime{Set: true, Null: event.CurrentTime == nil}
		if event.CurrentTime != nil {
			result.CurrentTime.Value = apiDataBroadcastCurrentTime(event.CurrentTime)
		}
	case "esEventUpdated":
		result.EsEvent = apigen.OptNilDataBroadcastESEvent{Set: true, Null: event.ESEvent == nil}
		if event.ESEvent != nil {
			result.EsEvent.Value = apiDataBroadcastESEvent(event.ESEvent)
		}
	case "bit":
		result.Bit = apigen.OptNilDataBroadcastBIT{Set: true, Null: event.BIT == nil}
		if event.BIT != nil {
			result.Bit.Value = apiDataBroadcastBIT(event.BIT)
		}
	case "pcr":
		result.Pcr = apigen.OptNilDataBroadcastPCR{Set: true, Null: event.PCR == nil}
		if event.PCR != nil {
			result.Pcr.Value = apiDataBroadcastPCR(event.PCR)
		}
	}
	return result
}

// apiDataBroadcastSnapshot renders a snapshot. origin is live for state read
// from an active channel session, or cache for a provisional snapshot rebuilt
// from persisted PMT/DII sections without a tuner. storedAtUnixMilli is nil
// for a live snapshot; a cache snapshot always carries it, and always has
// null programInfo/currentTime/pcr (see RestoreSnapshot).
func apiDataBroadcastSnapshot(serviceItemID int64, snapshot bml.Snapshot, origin apigen.DataBroadcastSnapshotOrigin, storedAtUnixMilli *int64) apigen.DataBroadcastSnapshot {
	result := apigen.DataBroadcastSnapshot{
		ServiceId:  int(snapshot.ServiceID),
		Revision:   int64(snapshot.Revision),
		Origin:     origin,
		StoredAt:   nilInt64(storedAtUnixMilli),
		Pmt:        apigen.NilDataBroadcastPMT{Null: snapshot.PMT == nil},
		Components: apiDataBroadcastComponents(serviceItemID, snapshot.Components),
		ProgramInfo: apigen.NilDataBroadcastProgramInfo{
			Null: snapshot.ProgramInfo == nil,
		},
		CurrentTime: apigen.NilDataBroadcastCurrentTime{Null: snapshot.CurrentTime == nil},
		Bit:         apigen.NilDataBroadcastBIT{Null: snapshot.BIT == nil},
		Pcr:         apigen.NilDataBroadcastPCR{Null: snapshot.PCR == nil},
	}
	if snapshot.PMT != nil {
		result.Pmt.Value = apiDataBroadcastPMT(serviceItemID, snapshot.PMT)
	}
	if snapshot.ProgramInfo != nil {
		result.ProgramInfo.Value = apiDataBroadcastProgramInfo(snapshot.ProgramInfo)
	}
	if snapshot.CurrentTime != nil {
		result.CurrentTime.Value = apiDataBroadcastCurrentTime(snapshot.CurrentTime)
	}
	if snapshot.BIT != nil {
		result.Bit.Value = apiDataBroadcastBIT(snapshot.BIT)
	}
	if snapshot.PCR != nil {
		result.Pcr.Value = apiDataBroadcastPCR(snapshot.PCR)
	}
	return result
}

func apiDataBroadcastProgramInfo(info *bml.ProgramInfo) apigen.DataBroadcastProgramInfo {
	var eventIDs []int
	if info.EventIDs != nil {
		eventIDs = make([]int, len(info.EventIDs))
		for i, id := range info.EventIDs {
			eventIDs[i] = int(id)
		}
	}
	return apigen.DataBroadcastProgramInfo{ServiceId: int(info.ServiceID), EventIds: eventIDs, RawSectionHex: info.RawSectionHex}
}

func apiDataBroadcastCurrentTime(current *bml.CurrentTime) apigen.DataBroadcastCurrentTime {
	return apigen.DataBroadcastCurrentTime{JstTimeUnixMilli: current.JSTTimeUnixMilli}
}

func apiDataBroadcastPCR(pcr *bml.PCR) apigen.DataBroadcastPCR {
	return apigen.DataBroadcastPCR{PcrBase: int64(pcr.PCRBase), PcrExtension: int(pcr.PCRExtension)}
}

func apiDataBroadcastESEvent(event *bml.ESEvent) apigen.DataBroadcastESEvent {
	events := make([]apigen.DataBroadcastGeneralEvent, 0, len(event.Events))
	for _, item := range event.Events {
		value := apigen.DataBroadcastGeneralEvent{Type: item.Type}
		if npt := item.NPTReference; npt != nil {
			value.PostDiscontinuityIndicator = apigen.NewOptBool(npt.PostDiscontinuityIndicator)
			value.DsmContentId = apigen.NewOptInt(int(npt.DSMContentID))
			value.STCReference = apigen.NewOptInt64(int64(npt.STCReference))
			value.NPTReference = apigen.NewOptInt64(int64(npt.NPTReference))
			value.ScaleNumerator = apigen.NewOptInt(int(npt.ScaleNumerator))
			value.ScaleDenominator = apigen.NewOptInt(int(npt.ScaleDenominator))
		} else {
			value.EventMessageGroupId = apigen.NewOptInt(int(item.EventMessageGroupID))
			value.TimeMode = apigen.NewOptInt(int(item.TimeMode))
			value.EventMessageType = apigen.NewOptInt(int(item.EventMessageType))
			value.EventMessageId = apigen.NewOptInt(int(item.EventMessageID))
			value.PrivateDataByte = bytesToNumbers(item.PrivateData)
			if item.EventMessageNPT != nil {
				value.EventMessageNPT = apigen.NewOptInt64(int64(*item.EventMessageNPT))
			}
		}
		events = append(events, value)
	}
	return apigen.DataBroadcastESEvent{ComponentId: int(event.ComponentTag), DataEventId: int(event.DataEventID), Events: events}
}

func apiDataBroadcastBIT(bit *bml.BIT) apigen.DataBroadcastBIT {
	broadcasters := make([]apigen.DataBroadcastBroadcaster, 0, len(bit.Broadcasters))
	for _, broadcaster := range bit.Broadcasters {
		services := make([]apigen.DataBroadcastBITService, 0, len(broadcaster.Services))
		for _, service := range broadcaster.Services {
			services = append(services, apigen.DataBroadcastBITService{ServiceId: int(service.ServiceID), ServiceType: int(service.ServiceType)})
		}
		affiliated := make([]apigen.DataBroadcastAffiliatedBroadcaster, 0, len(broadcaster.AffiliationBroadcasters))
		for _, item := range broadcaster.AffiliationBroadcasters {
			affiliated = append(affiliated, apigen.DataBroadcastAffiliatedBroadcaster{OriginalNetworkId: int(item.OriginalNetworkID), BroadcasterId: int(item.BroadcasterID)})
		}
		broadcasters = append(broadcasters, apigen.DataBroadcastBroadcaster{
			BroadcasterId:            int(broadcaster.BroadcasterID),
			BroadcasterName:          nilString(broadcaster.BroadcasterName),
			Services:                 services,
			Affiliations:             bytesToNumbers(broadcaster.Affiliations),
			AffiliationBroadcasters:  affiliated,
			TerrestrialBroadcasterId: nilInt(broadcaster.TerrestrialBroadcasterID),
		})
	}
	return apigen.DataBroadcastBIT{OriginalNetworkId: int(bit.OriginalNetworkID), Version: int(bit.Version), Broadcasters: broadcasters, RawSectionHex: bit.RawSectionHex}
}

func bytesToNumbers(values []byte) []int {
	result := make([]int, len(values))
	for i, value := range values {
		result[i] = int(value)
	}
	return result
}

func apiDataBroadcastPMT(serviceItemID int64, pmt *bml.PMT) apigen.DataBroadcastPMT {
	return apigen.DataBroadcastPMT{
		ServiceId:     int(pmt.ServiceID),
		Version:       int(pmt.Version),
		PcrPid:        int(pmt.PCRPID),
		Components:    apiDataBroadcastComponents(serviceItemID, pmt.Components),
		RawSectionHex: pmt.RawSectionHex,
	}
}

func apiDataBroadcastComponents(serviceItemID int64, components []bml.Component) []apigen.DataBroadcastComponent {
	result := make([]apigen.DataBroadcastComponent, 0, len(components))
	for _, component := range components {
		item := apigen.DataBroadcastComponent{
			ComponentTag:    int(component.ComponentTag),
			Pid:             int(component.PID),
			StreamType:      int(component.StreamType),
			DataComponentId: nilInt(component.DataComponentID),
			BxmlInfo:        apigen.NilDataBroadcastBXMLInfo{Null: component.BXMLInfo == nil},
			DataEventId:     int(component.DataEventID),
			ReturnToEntry:   apigen.NilDataBroadcastReturnToEntry{Null: component.ReturnToEntry == nil},
			Carousel: apigen.DataBroadcastCarousel{
				Status:     component.CarouselStatus,
				DownloadId: nilInt64(component.CarouselDownloadID),
				BlockSize:  nilInt(component.CarouselBlockSize),
			},
			Modules: apiDataBroadcastModules(serviceItemID, component.Modules),
		}
		if component.BXMLInfo != nil {
			item.BxmlInfo.Value = apiDataBroadcastBXMLInfo(component.BXMLInfo)
		}
		if component.ReturnToEntry != nil {
			item.ReturnToEntry.Value = apigen.DataBroadcastReturnToEntry(*component.ReturnToEntry)
		}
		result = append(result, item)
	}
	return result
}

func apiDataBroadcastBXMLInfo(info *bml.BXMLInfo) apigen.DataBroadcastBXMLInfo {
	result := apigen.DataBroadcastBXMLInfo{TransmissionFormat: int(info.TransmissionFormat), EntryPointFlag: info.EntryPointFlag}
	if entry := info.EntryPointInfo; entry != nil {
		result.EntryPointInfo = apigen.NewOptDataBroadcastBXMLEntryPoint(apigen.DataBroadcastBXMLEntryPoint{
			AutoStartFlag:      entry.AutoStartFlag,
			DocumentResolution: int(entry.DocumentResolution),
			UseXML:             entry.UseXML,
			DefaultVersionFlag: entry.DefaultVersionFlag,
			IndependentFlag:    entry.IndependentFlag,
			StyleForTVFlag:     entry.StyleForTVFlag,
			BmlMajorVersion:    int(entry.BMLMajorVersion),
			BmlMinorVersion:    int(entry.BMLMinorVersion),
			BxmlMajorVersion:   nilInt(entry.BXMLMajorVersion),
			BxmlMinorVersion:   nilInt(entry.BXMLMinorVersion),
		})
	}
	if carousel := info.AdditionalAribCarouselInfo; carousel != nil {
		result.AdditionalAribCarouselInfo = apigen.NewOptDataBroadcastBXMLCarousel(apigen.DataBroadcastBXMLCarousel{
			DataEventId:           int(carousel.DataEventID),
			EventSectionFlag:      carousel.EventSectionFlag,
			OndemandRetrievalFlag: carousel.OnDemandRetrievalFlag,
			FileStorableFlag:      carousel.FileStorableFlag,
			StartPriority:         int(carousel.StartPriority),
		})
	}
	return result
}

func apiDataBroadcastModuleList(serviceItemID int64, list *bml.ModuleList) apigen.DataBroadcastModuleList {
	result := apigen.DataBroadcastModuleList{
		ComponentTag:  int(list.ComponentTag),
		DownloadId:    int64(list.DownloadID),
		BlockSize:     int(list.BlockSize),
		DataEventId:   int(list.DataEventID),
		ReturnToEntry: apigen.NilBool{Null: list.ReturnToEntry == nil},
		Modules:       apiDataBroadcastModules(serviceItemID, list.Modules),
	}
	if list.ReturnToEntry != nil {
		result.ReturnToEntry.Value = *list.ReturnToEntry
	}
	return result
}

func apiDataBroadcastModules(serviceItemID int64, modules []bml.Module) []apigen.DataBroadcastModule {
	result := make([]apigen.DataBroadcastModule, 0, len(modules))
	for i := range modules {
		result = append(result, apiDataBroadcastModule(serviceItemID, &modules[i]))
	}
	return result
}

// dataBroadcastModuleURL is the path of a module generation. It is rooted at
// the API mount, as the API description states.
func dataBroadcastModuleURL(serviceItemID int64, module *bml.Module) string {
	return fmt.Sprintf("/api/services/%d/data-broadcast/bml/components/%d/carousels/%d/modules/%d/versions/%d", serviceItemID, module.ComponentTag, module.DownloadID, module.ModuleID, module.Version)
}

func apiDataBroadcastModule(serviceItemID int64, module *bml.Module) apigen.DataBroadcastModule {
	result := apigen.DataBroadcastModule{
		ComponentTag:    int(module.ComponentTag),
		ModuleId:        int(module.ModuleID),
		DownloadId:      int64(module.DownloadID),
		Version:         int(module.Version),
		Size:            int64(module.Size),
		Info:            module.Info,
		Metadata:        apigen.NilDataBroadcastModuleMetadata{Null: module.Metadata == nil},
		Complete:        module.Complete,
		Status:          module.Status,
		RejectionReason: nilString(module.RejectionReason),
		ReceivedBlocks:  module.ReceivedBlocks,
		TotalBlocks:     module.TotalBlocks,
		Etag:            module.ETag,
		URL:             dataBroadcastModuleURL(serviceItemID, module),
	}
	if module.Metadata != nil {
		result.Metadata.Value = apiDataBroadcastModuleMetadata(module.Metadata)
	}
	return result
}

func apiDataBroadcastModuleManifest(serviceItemID int64, module bml.Module, resources []resource.ModuleResource) apigen.DataBroadcastModuleManifest {
	base := dataBroadcastModuleURL(serviceItemID, &module)
	items := make([]apigen.DataBroadcastModuleResource, 0, len(resources))
	for _, resource := range resources {
		items = append(items, apigen.DataBroadcastModuleResource{
			ID:              resource.ID,
			ContentLocation: nilString(resource.ContentLocation),
			ContentType:     resource.ContentType,
			URL:             base + "/resources/" + resource.ID,
		})
	}
	return apigen.DataBroadcastModuleManifest{
		ComponentTag: int(module.ComponentTag),
		DownloadId:   int64(module.DownloadID),
		ModuleId:     int(module.ModuleID),
		Version:      int(module.Version),
		Size:         int64(module.Size),
		Etag:         module.ETag,
		RawUrl:       base + "/raw",
		Resources:    items,
	}
}

func apiDataBroadcastModuleMetadata(metadata *bml.ModuleMetadata) apigen.DataBroadcastModuleMetadata {
	return apigen.DataBroadcastModuleMetadata{
		Type:                     metadata.Type,
		Name:                     metadata.Name,
		Crc32:                    nilInt64(metadata.CRC32),
		EstimatedDownloadSeconds: nilInt64(metadata.EstimatedDownloadSeconds),
		CachingPriority:          nilInt(metadata.CachingPriority),
		ExpireMode:               nilInt(metadata.ExpireMode),
		ExpireDataByte:           bytesToNumbers(metadata.ExpireData),
		ActivationMode:           nilInt(metadata.ActivationMode),
		ActivationDataByte:       bytesToNumbers(metadata.ActivationData),
		CompressionType:          nilInt(metadata.CompressionType),
		OriginalSize:             nilInt64(metadata.OriginalSize),
	}
}

type integer interface {
	~uint8 | ~uint16 | ~uint32 | ~int64
}

// nilInt converts an optional broadcast value to a nullable API integer.
func nilInt[T integer](value *T) apigen.NilInt {
	if value == nil {
		return apigen.NilInt{Null: true}
	}
	return apigen.NewNilInt(int(*value))
}

func nilInt64[T integer](value *T) apigen.NilInt64 {
	if value == nil {
		return apigen.NilInt64{Null: true}
	}
	return apigen.NewNilInt64(int64(*value))
}

func nilString(value *string) apigen.NilString {
	if value == nil {
		return apigen.NilString{Null: true}
	}
	return apigen.NewNilString(*value)
}
