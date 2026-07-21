package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/21S1298001/mahiron/ts"
)

func GetServiceDataBroadcastEvents(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastEventsParams, w http.ResponseWriter) error {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return err
	}
	if service == nil {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	decode := shouldDecode(params.Decode)
	serviceID := service.ServiceId
	networkID := service.NetworkId
	ctx, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(service.ChannelType, service.ChannelId), &networkID, &serviceID)
	session, err := h.streamManager.GetOrCreate(ctx, service.ChannelType, service.ChannelId)
	if err != nil {
		if errors.Is(err, stream.ErrChannelNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return nil
		}
		if errors.Is(err, stream.ErrTunerNotFound) || errors.Is(err, stream.ErrUnsupportedTuner) || errors.Is(err, stream.ErrTunerUnavailable) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return nil
		}
		return err
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Mirakurun-Tuner-User-ID", userID)
	w.WriteHeader(http.StatusOK)
	flusher := flushWriter{w: w}
	return session.ObserveDataBroadcast(ctx, service.ServiceId, decode, func(event databroadcast.DataBroadcastEvent) error {
		return writeDataBroadcastSSE(flusher, params.ID, event)
	})
}

// GetServiceDataBroadcastState returns the authoritative state without
// allocating a tuner. Clients fetch it before opening SSE and after reconnect.
func GetServiceDataBroadcastState(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastStateParams, w http.ResponseWriter) error {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return err
	}
	if service == nil {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	session, ok := h.streamManager.GetExisting(service.ChannelType, service.ChannelId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	return json.NewEncoder(w).Encode(apiDataBroadcastSnapshot(params.ID, session.DataBroadcastSnapshot(service.ServiceId)))
}

func GetServiceDataBroadcastModuleVersion(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastModuleVersionParams, w http.ResponseWriter) error {
	module, status, err := dataBroadcastVersionModule(ctx, h, params.ID, byte(params.ComponentTag), uint32(params.DownloadId), uint16(params.ModuleId), byte(params.ModuleVersion))
	if err != nil || status != 0 {
		if status != 0 {
			w.WriteHeader(status)
		}
		return err
	}
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	w.Header().Set("ETag", module.ETag)
	if value, ok := params.IfNoneMatch.Get(); ok && etagMatches(value, module.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	resources, err := dataBroadcastModuleResources(ctx, h, params.ID, module)
	if err != nil {
		return writeModuleDecodeError(w, err)
	}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(apiDataBroadcastModuleManifest(params.ID, module, resources))
}

func GetServiceDataBroadcastModuleRaw(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastModuleRawParams, w http.ResponseWriter) error {
	module, status, err := dataBroadcastVersionModule(ctx, h, params.ID, byte(params.ComponentTag), uint32(params.DownloadId), uint16(params.ModuleId), byte(params.ModuleVersion))
	if err != nil || status != 0 {
		if status != 0 {
			w.WriteHeader(status)
		}
		return err
	}
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	w.Header().Set("ETag", module.ETag)
	if value, ok := params.IfNoneMatch.Get(); ok && etagMatches(value, module.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(module.Data)
	return err
}

func GetServiceDataBroadcastModuleResource(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastModuleResourceParams, w http.ResponseWriter) error {
	module, status, err := dataBroadcastVersionModule(ctx, h, params.ID, byte(params.ComponentTag), uint32(params.DownloadId), uint16(params.ModuleId), byte(params.ModuleVersion))
	if err != nil || status != 0 {
		if status != 0 {
			w.WriteHeader(status)
		}
		return err
	}
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	w.Header().Set("ETag", module.ETag)
	if value, ok := params.IfNoneMatch.Get(); ok && etagMatches(value, module.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	resources, err := dataBroadcastModuleResources(ctx, h, params.ID, module)
	if err != nil {
		return writeModuleDecodeError(w, err)
	}
	for _, resource := range resources {
		if resource.ID != params.ResourceId {
			continue
		}
		w.Header().Set("Content-Type", resource.ContentType)
		_, err = w.Write(resource.Data)
		return err
	}
	w.WriteHeader(http.StatusNotFound)
	return nil
}

func writeModuleDecodeError(w http.ResponseWriter, err error) error {
	if errors.Is(err, databroadcast.ErrModuleResourceLimit) {
		w.WriteHeader(http.StatusInsufficientStorage)
		return nil
	}
	if errors.Is(err, databroadcast.ErrMalformedModule) || errors.Is(err, databroadcast.ErrUnsupportedModuleCompression) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return nil
	}
	return err
}

func dataBroadcastModuleResources(ctx context.Context, h *Handler, serviceItemID int64, module databroadcast.DataBroadcastModule) ([]databroadcast.ModuleResource, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(serviceItemID, 10))
	if err != nil {
		return nil, err
	}
	if service != nil {
		if store, ok := h.streamManager.(interface {
			DataBroadcastCachedResources(string, string, uint16, byte, uint32, uint16, byte) ([]databroadcast.ModuleResource, bool)
		}); ok {
			if resources, found := store.DataBroadcastCachedResources(service.ChannelType, service.ChannelId, service.ServiceId, module.ComponentTag, module.DownloadID, module.ModuleID, module.Version); found {
				return resources, nil
			}
		}
	}
	return databroadcast.DecodeModuleResources(module)
}

func dataBroadcastVersionModule(ctx context.Context, h *Handler, serviceItemID int64, componentTag byte, downloadID uint32, moduleID uint16, version byte) (databroadcast.DataBroadcastModule, int, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(serviceItemID, 10))
	if err != nil {
		return databroadcast.DataBroadcastModule{}, 0, err
	}
	if service == nil {
		return databroadcast.DataBroadcastModule{}, http.StatusNotFound, nil
	}
	if session, ok := h.streamManager.GetExisting(service.ChannelType, service.ChannelId); ok {
		module, found := session.DataBroadcastModuleVersion(service.ServiceId, componentTag, downloadID, moduleID, version)
		if found {
			return module, 0, nil
		}
		if announced, rejected := announcedModuleVersion(session.DataBroadcastSnapshot(service.ServiceId), componentTag, downloadID, moduleID, version); rejected {
			return databroadcast.DataBroadcastModule{}, http.StatusInsufficientStorage, nil
		} else if announced {
			return databroadcast.DataBroadcastModule{}, http.StatusTooEarly, nil
		}
	}
	if store, ok := h.streamManager.(interface {
		DataBroadcastCachedModule(string, string, uint16, byte, uint32, uint16, byte) (databroadcast.DataBroadcastModule, bool)
	}); ok {
		if module, found := store.DataBroadcastCachedModule(service.ChannelType, service.ChannelId, service.ServiceId, componentTag, downloadID, moduleID, version); found {
			return module, 0, nil
		}
	}
	if store, ok := h.streamManager.(interface {
		DataBroadcastModuleWasEvicted(string, string, uint16, byte, uint32, uint16, byte) bool
	}); ok && store.DataBroadcastModuleWasEvicted(service.ChannelType, service.ChannelId, service.ServiceId, componentTag, downloadID, moduleID, version) {
		return databroadcast.DataBroadcastModule{}, http.StatusGone, nil
	}
	return databroadcast.DataBroadcastModule{}, http.StatusNotFound, nil
}

func announcedModuleVersion(snapshot databroadcast.DataBroadcastSnapshot, componentTag byte, downloadID uint32, moduleID uint16, version byte) (bool, bool) {
	for _, component := range snapshot.Components {
		if component.ComponentTag != componentTag {
			continue
		}
		for _, module := range component.Modules {
			if module.DownloadID == downloadID && module.ModuleID == moduleID && module.Version == version {
				return true, module.Status == "rejected"
			}
		}
	}
	return false, false
}

func etagMatches(ifNoneMatch, etag string) bool {
	for value := range strings.SplitSeq(ifNoneMatch, ",") {
		value = strings.TrimSpace(value)
		if value == "*" || value == etag || strings.TrimPrefix(value, "W/") == etag {
			return true
		}
	}
	return false
}

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
		result["snapshot"] = apiDataBroadcastSnapshot(serviceItemID, event.Snapshot)
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

func apiDataBroadcastSnapshot(serviceItemID int64, snapshot databroadcast.DataBroadcastSnapshot) map[string]any {
	return map[string]any{
		"serviceId":   snapshot.ServiceID,
		"revision":    snapshot.Revision,
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
