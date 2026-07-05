package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/21S1298001/mahiron/internal/stream"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
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
	return session.ObserveDataBroadcast(ctx, service.ServiceId, decode, func(event stream.DataBroadcastEvent) error {
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
	if value, ok := params.IfNoneMatch.Get(); ok && value == module.ETag {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", module.ETag)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(module.Data)
	return err
}

func writeDataBroadcastSSE(w io.Writer, serviceItemID int64, event stream.DataBroadcastEvent) error {
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

func apiDataBroadcastEvent(serviceItemID int64, event stream.DataBroadcastEvent) map[string]any {
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
	}
	return result
}

func apiDataBroadcastSnapshot(serviceItemID int64, snapshot stream.DataBroadcastSnapshot) map[string]any {
	return map[string]any{
		"serviceId":   snapshot.ServiceID,
		"pmt":         apiDataBroadcastPMT(serviceItemID, snapshot.PMT),
		"components":  apiDataBroadcastComponents(serviceItemID, snapshot.Components),
		"programInfo": snapshot.ProgramInfo,
		"currentTime": snapshot.CurrentTime,
	}
}

func apiDataBroadcastPMT(serviceItemID int64, pmt *stream.DataBroadcastPMT) any {
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

func apiDataBroadcastComponents(serviceItemID int64, components []stream.DataBroadcastComponent) []map[string]any {
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

func apiDataBroadcastModuleList(serviceItemID int64, list *stream.DataBroadcastModuleList) any {
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

func apiDataBroadcastModule(serviceItemID int64, module *stream.DataBroadcastModule) map[string]any {
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
		"complete":     module.Complete,
		"etag":         module.ETag,
		"url":          fmt.Sprintf("/api/services/%d/data-broadcast/modules/%d/%d", serviceItemID, module.ComponentTag, module.ModuleID),
	}
}
