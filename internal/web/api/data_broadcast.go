package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func GetServiceDataBroadcastEvents(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastEventsParams, w http.ResponseWriter) error {
	if h.dataBroadcastDisabled {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
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
// When no channel session currently exists, it falls back to a provisional
// snapshot rebuilt from persisted PMT/DII state (origin "cache") unless the
// caller passes allowCache=0.
func GetServiceDataBroadcastState(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastStateParams, w http.ResponseWriter) error {
	if h.dataBroadcastDisabled {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return err
	}
	if service == nil {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	if session, ok := h.streamManager.GetExisting(service.ChannelType, service.ChannelId); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		return json.NewEncoder(w).Encode(apiDataBroadcastSnapshot(params.ID, session.DataBroadcastSnapshot(service.ServiceId), "live", nil))
	}
	if snapshot, storedAtUnixMilli, found := provisionalDataBroadcastSnapshot(h, params.AllowCache, service.ChannelType, service.ChannelId, service.ServiceId); found {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		return json.NewEncoder(w).Encode(apiDataBroadcastSnapshot(params.ID, snapshot, "cache", &storedAtUnixMilli))
	}
	w.WriteHeader(http.StatusNotFound)
	return nil
}

func provisionalDataBroadcastSnapshot(h *Handler, allowCache apigen.OptInt, channelType, channelID string, serviceID uint16) (databroadcast.DataBroadcastSnapshot, int64, bool) {
	if !shouldAllowCache(allowCache) {
		return databroadcast.DataBroadcastSnapshot{}, 0, false
	}
	store, ok := h.streamManager.(interface {
		DataBroadcastProvisionalSnapshot(string, string, uint16) (databroadcast.DataBroadcastSnapshot, int64, bool)
	})
	if !ok {
		return databroadcast.DataBroadcastSnapshot{}, 0, false
	}
	snapshot, storedAt, found := store.DataBroadcastProvisionalSnapshot(channelType, channelID, serviceID)
	if !found {
		return databroadcast.DataBroadcastSnapshot{}, 0, false
	}
	return snapshot, storedAt * 1000, true
}

func GetServiceDataBroadcastModuleVersion(ctx context.Context, h *Handler, params apigen.GetServiceDataBroadcastModuleVersionParams, w http.ResponseWriter) error {
	if h.dataBroadcastDisabled {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
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
	if h.dataBroadcastDisabled {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
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
	if h.dataBroadcastDisabled {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
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
