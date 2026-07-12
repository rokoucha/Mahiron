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

func GetServiceDataBroadcastModule(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastModuleParams, w http.ResponseWriter) error {
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
	module, ok := session.DataBroadcastModule(service.ServiceId, byte(params.ComponentTag), uint16(params.ModuleId))
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	// DSM-CC module URLs are stable while their contents are versioned by the
	// DII moduleVersion/downloadId pair represented in the ETag. Allow clients
	// to retain the bytes and revalidate them instead of downloading the same
	// carousel module for every BML reference.
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("ETag", module.ETag)
	if value, ok := params.IfNoneMatch.Get(); ok && etagMatches(value, module.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(module.Data)
	return err
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
	result := map[string]any{"type": event.Type}
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
		result["programInfo"] = event.ProgramInfo
	case "currentTime":
		result["currentTime"] = event.CurrentTime
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
		"pmt":         apiDataBroadcastPMT(serviceItemID, snapshot.PMT),
		"components":  apiDataBroadcastComponents(serviceItemID, snapshot.Components),
		"programInfo": snapshot.ProgramInfo,
		"currentTime": snapshot.CurrentTime,
		"bit":         apiDataBroadcastBIT(snapshot.BIT),
		"pcr":         apiDataBroadcastPCR(snapshot.PCR),
	}
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
			"componentTag": component.ComponentTag,
			"pid":          component.PID,
			"streamType":   component.StreamType,
			"modules":      modules,
		})
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
		"componentTag": list.ComponentTag,
		"downloadId":   list.DownloadID,
		"blockSize":    list.BlockSize,
		"modules":      modules,
	}
}

func apiDataBroadcastModule(serviceItemID int64, module *databroadcast.DataBroadcastModule) map[string]any {
	if module == nil {
		return nil
	}
	return map[string]any{
		"componentTag": module.ComponentTag,
		"moduleId":     module.ModuleID,
		"downloadId":   module.DownloadID,
		"version":      module.Version,
		"size":         module.Size,
		"info":         module.Info,
		"metadata":     apiDataBroadcastModuleMetadata(module.Metadata),
		"complete":     module.Complete,
		"etag":         module.ETag,
		"url":          fmt.Sprintf("/api/services/%d/data-broadcast/modules/%d/%d", serviceItemID, module.ComponentTag, module.ModuleID),
	}
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
