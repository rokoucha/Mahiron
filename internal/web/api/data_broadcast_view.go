package api

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/ts"
)

func writeDataBroadcastSSE(w io.Writer, serviceItemID int64, event databroadcast.DataBroadcastEvent) error {
	payload := apiDataBroadcastEvent(serviceItemID, event)
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", event.Type); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\n", event.Sequence); err != nil {
		return err
	}
	if _, err := w.Write([]byte("data: ")); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	_, err = w.Write([]byte("\n\n"))
	return err
}

func apiDataBroadcastEvent(serviceItemID int64, event databroadcast.DataBroadcastEvent) map[string]any {
	result := map[string]any{"type": event.Type, "sequence": event.Sequence, "revision": event.Revision}
	switch event.Type {
	case "snapshot":
		result["snapshot"] = apiDataBroadcastSnapshot(serviceItemID, event.Snapshot, "live", nil)
	case "pmt":
		result["pmt"] = apiDataBroadcastPMT(serviceItemID, event.PMT)
	case "moduleListUpdated":
		result["moduleList"] = apiDataBroadcastModuleList(serviceItemID, event.ModuleList)
	case "moduleUpdated":
		result["module"] = apiDataBroadcastModule(serviceItemID, event.Module)
	case "programInfo":
		result["programInfo"] = apiDataBroadcastProgramInfo(event.ProgramInfo)
	case "currentTime":
		result["currentTime"] = apiDataBroadcastCurrentTime(event.CurrentTime)
	case "esEventUpdated":
		result["esEvent"] = apiDataBroadcastESEvent(event.ESEvent)
	case "bit":
		result["bit"] = apiDataBroadcastBIT(event.BIT)
	case "pcr":
		result["pcr"] = apiDataBroadcastPCR(event.PCR)
	}
	return result
}

// apiDataBroadcastSnapshot renders a snapshot. origin is "live" for state
// read from an active channel session, or "cache" for a provisional snapshot
// rebuilt from persisted PMT/DII sections without a tuner. storedAtUnixMilli
// is nil for a live snapshot; a cache snapshot always carries it, and always
// has null programInfo/currentTime/pcr (see RestoreSnapshot).
func apiDataBroadcastSnapshot(serviceItemID int64, snapshot databroadcast.DataBroadcastSnapshot, origin string, storedAtUnixMilli *int64) map[string]any {
	return map[string]any{
		"serviceId":   snapshot.ServiceID,
		"revision":    snapshot.Revision,
		"origin":      origin,
		"storedAt":    storedAtUnixMilli,
		"pmt":         apiDataBroadcastPMT(serviceItemID, snapshot.PMT),
		"components":  apiDataBroadcastComponents(serviceItemID, snapshot.Components),
		"programInfo": apiDataBroadcastProgramInfo(snapshot.ProgramInfo),
		"currentTime": apiDataBroadcastCurrentTime(snapshot.CurrentTime),
		"bit":         apiDataBroadcastBIT(snapshot.BIT),
		"pcr":         apiDataBroadcastPCR(snapshot.PCR),
	}
}

func apiDataBroadcastProgramInfo(info *databroadcast.DataBroadcastProgramInfo) any {
	if info == nil {
		return nil
	}
	return map[string]any{"serviceId": info.ServiceID, "eventIds": info.EventIDs, "rawSectionHex": info.RawSectionHex}
}

func apiDataBroadcastCurrentTime(current *databroadcast.DataBroadcastCurrentTime) any {
	if current == nil {
		return nil
	}
	return map[string]any{"jstTimeUnixMilli": current.JSTTimeUnixMilli}
}

func apiDataBroadcastPCR(pcr *databroadcast.DataBroadcastPCR) any {
	if pcr == nil {
		return nil
	}
	return map[string]any{"pcrBase": pcr.PCRBase, "pcrExtension": pcr.PCRExtension}
}

func apiDataBroadcastESEvent(event *databroadcast.DataBroadcastESEvent) any {
	if event == nil {
		return nil
	}
	events := make([]map[string]any, 0, len(event.Events))
	for _, item := range event.Events {
		value := map[string]any{"type": item.Type}
		if item.NPTReference != nil {
			value["postDiscontinuityIndicator"] = item.NPTReference.PostDiscontinuityIndicator
			value["dsmContentId"] = item.NPTReference.DSMContentID
			value["STCReference"] = item.NPTReference.STCReference
			value["NPTReference"] = item.NPTReference.NPTReference
			value["scaleNumerator"] = item.NPTReference.ScaleNumerator
			value["scaleDenominator"] = item.NPTReference.ScaleDenominator
		} else {
			value["eventMessageGroupId"] = item.EventMessageGroupID
			value["timeMode"] = item.TimeMode
			value["eventMessageType"] = item.EventMessageType
			value["eventMessageId"] = item.EventMessageID
			value["privateDataByte"] = bytesToNumbers(item.PrivateData)
			if item.EventMessageNPT != nil {
				value["eventMessageNPT"] = *item.EventMessageNPT
			}
		}
		events = append(events, value)
	}
	return map[string]any{"componentId": event.ComponentTag, "dataEventId": event.DataEventID, "events": events}
}

func apiDataBroadcastBIT(bit *databroadcast.DataBroadcastBIT) any {
	if bit == nil {
		return nil
	}
	broadcasters := make([]map[string]any, 0, len(bit.Broadcasters))
	for _, broadcaster := range bit.Broadcasters {
		services := make([]map[string]any, 0, len(broadcaster.Services))
		for _, service := range broadcaster.Services {
			services = append(services, map[string]any{"serviceId": service.ServiceID, "serviceType": service.ServiceType})
		}
		affiliated := make([]map[string]any, 0, len(broadcaster.AffiliationBroadcasters))
		for _, item := range broadcaster.AffiliationBroadcasters {
			affiliated = append(affiliated, map[string]any{"originalNetworkId": item.OriginalNetworkID, "broadcasterId": item.BroadcasterID})
		}
		broadcasters = append(broadcasters, map[string]any{
			"broadcasterId": broadcaster.BroadcasterID, "broadcasterName": broadcaster.BroadcasterName,
			"services": services, "affiliations": bytesToNumbers(broadcaster.Affiliations),
			"affiliationBroadcasters": affiliated, "terrestrialBroadcasterId": broadcaster.TerrestrialBroadcasterID,
		})
	}
	return map[string]any{"originalNetworkId": bit.OriginalNetworkID, "version": bit.Version, "broadcasters": broadcasters, "rawSectionHex": bit.RawSectionHex}
}

func bytesToNumbers(values []byte) []int {
	result := make([]int, len(values))
	for i, value := range values {
		result[i] = int(value)
	}
	return result
}

func apiDataBroadcastPMT(serviceItemID int64, pmt *databroadcast.DataBroadcastPMT) any {
	if pmt == nil {
		return nil
	}
	return map[string]any{
		"serviceId":     pmt.ServiceID,
		"version":       pmt.Version,
		"pcrPid":        pmt.PCRPID,
		"components":    apiDataBroadcastComponents(serviceItemID, pmt.Components),
		"rawSectionHex": pmt.RawSectionHex,
	}
}

func apiDataBroadcastComponents(serviceItemID int64, components []databroadcast.DataBroadcastComponent) []map[string]any {
	result := make([]map[string]any, 0, len(components))
	for _, component := range components {
		modules := make([]map[string]any, 0, len(component.Modules))
		for i := range component.Modules {
			modules = append(modules, apiDataBroadcastModule(serviceItemID, &component.Modules[i]))
		}
		result = append(result, map[string]any{
			"componentTag":    component.ComponentTag,
			"pid":             component.PID,
			"streamType":      component.StreamType,
			"dataComponentId": component.DataComponentID,
			"bxmlInfo":        apiAdditionalAribBXMLInfo(component.BXMLInfo),
			"dataEventId":     component.DataEventID,
			"returnToEntry":   component.ReturnToEntry,
			"carousel": map[string]any{
				"status": component.CarouselStatus, "downloadId": component.CarouselDownloadID,
				"blockSize": component.CarouselBlockSize,
			},
			"modules": modules,
		})
	}
	return result
}

func apiAdditionalAribBXMLInfo(info *ts.AdditionalAribBXMLInfo) any {
	if info == nil {
		return nil
	}
	result := map[string]any{"transmissionFormat": info.TransmissionFormat, "entryPointFlag": info.EntryPointFlag}
	if entry := info.EntryPointInfo; entry != nil {
		result["entryPointInfo"] = map[string]any{
			"autoStartFlag": entry.AutoStartFlag, "documentResolution": entry.DocumentResolution,
			"useXML": entry.UseXML, "defaultVersionFlag": entry.DefaultVersionFlag,
			"independentFlag": entry.IndependentFlag, "styleForTVFlag": entry.StyleForTVFlag,
			"bmlMajorVersion": entry.BMLMajorVersion, "bmlMinorVersion": entry.BMLMinorVersion,
			"bxmlMajorVersion": entry.BXMLMajorVersion, "bxmlMinorVersion": entry.BXMLMinorVersion,
		}
	}
	if carousel := info.AdditionalAribCarouselInfo; carousel != nil {
		result["additionalAribCarouselInfo"] = map[string]any{
			"dataEventId": carousel.DataEventID, "eventSectionFlag": carousel.EventSectionFlag,
			"ondemandRetrievalFlag": carousel.OnDemandRetrievalFlag, "fileStorableFlag": carousel.FileStorableFlag,
			"startPriority": carousel.StartPriority,
		}
	}
	return result
}

func apiDataBroadcastModuleList(serviceItemID int64, list *databroadcast.DataBroadcastModuleList) any {
	if list == nil {
		return nil
	}
	modules := make([]map[string]any, 0, len(list.Modules))
	for i := range list.Modules {
		modules = append(modules, apiDataBroadcastModule(serviceItemID, &list.Modules[i]))
	}
	return map[string]any{
		"componentTag":  list.ComponentTag,
		"downloadId":    list.DownloadID,
		"blockSize":     list.BlockSize,
		"dataEventId":   list.DataEventID,
		"returnToEntry": list.ReturnToEntry,
		"modules":       modules,
	}
}

func apiDataBroadcastModule(serviceItemID int64, module *databroadcast.DataBroadcastModule) map[string]any {
	if module == nil {
		return nil
	}
	return map[string]any{
		"componentTag":    module.ComponentTag,
		"moduleId":        module.ModuleID,
		"downloadId":      module.DownloadID,
		"version":         module.Version,
		"size":            module.Size,
		"info":            module.Info,
		"metadata":        apiDataBroadcastModuleMetadata(module.Metadata),
		"complete":        module.Complete,
		"status":          module.Status,
		"rejectionReason": module.RejectionReason,
		"receivedBlocks":  module.ReceivedBlocks,
		"totalBlocks":     module.TotalBlocks,
		"etag":            module.ETag,
		"url":             fmt.Sprintf("/api/services/%d/data-broadcast/components/%d/carousels/%d/modules/%d/versions/%d", serviceItemID, module.ComponentTag, module.DownloadID, module.ModuleID, module.Version),
	}
}

func apiDataBroadcastModuleManifest(serviceItemID int64, module databroadcast.DataBroadcastModule, resources []databroadcast.ModuleResource) map[string]any {
	base := fmt.Sprintf("/api/services/%d/data-broadcast/components/%d/carousels/%d/modules/%d/versions/%d", serviceItemID, module.ComponentTag, module.DownloadID, module.ModuleID, module.Version)
	items := make([]map[string]any, 0, len(resources))
	for _, resource := range resources {
		items = append(items, map[string]any{"id": resource.ID, "contentLocation": resource.ContentLocation, "contentType": resource.ContentType, "url": base + "/resources/" + resource.ID})
	}
	return map[string]any{"componentTag": module.ComponentTag, "downloadId": module.DownloadID, "moduleId": module.ModuleID, "version": module.Version, "size": module.Size, "etag": module.ETag, "rawUrl": base + "/raw", "resources": items}
}

func apiDataBroadcastModuleMetadata(metadata *ts.DSMCCModuleMetadata) any {
	if metadata == nil {
		return nil
	}
	return map[string]any{
		"type":                     metadata.Type,
		"name":                     metadata.Name,
		"crc32":                    metadata.CRC32,
		"estimatedDownloadSeconds": metadata.EstimatedDownloadSeconds,
		"cachingPriority":          metadata.CachingPriority,
		"expireMode":               metadata.ExpireMode,
		"expireDataByte":           bytesToNumbers(metadata.ExpireData),
		"activationMode":           metadata.ActivationMode,
		"activationDataByte":       bytesToNumbers(metadata.ActivationData),
		"compressionType":          metadata.CompressionType,
		"originalSize":             metadata.OriginalSize,
	}
}
