package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/21S1298001/mahiron/internal/bml"
	"github.com/21S1298001/mahiron/internal/bml/cache"
	"github.com/21S1298001/mahiron/internal/bml/resource"
	"github.com/21S1298001/mahiron/internal/stream"
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
	serviceID := service.Key.ServiceID
	networkID := service.Key.NetworkID
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
	bmlSession, ok := session.(stream.BMLSource)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Mirakurun-Tuner-User-ID", userID)
	w.WriteHeader(http.StatusOK)
	flusher := flushWriter{w: w}
	return bmlSession.ObserveDataBroadcast(ctx, service.Key.ServiceID, decode, func(event bml.Event) error {
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
		bmlSession, ok := session.(stream.BMLSource)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return nil
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		snapshot := apiDataBroadcastSnapshot(params.ID, bmlSession.DataBroadcastSnapshot(service.Key.ServiceID), apigen.DataBroadcastSnapshotOriginLive, nil)
		return writeDataBroadcastJSON(w, &snapshot)
	}
	if snapshot, storedAtUnixMilli, found := provisionalDataBroadcastSnapshot(h, params.AllowCache, service.ChannelType, service.ChannelId, service.Key.ServiceID); found {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		api := apiDataBroadcastSnapshot(params.ID, snapshot, apigen.DataBroadcastSnapshotOriginCache, &storedAtUnixMilli)
		return writeDataBroadcastJSON(w, &api)
	}
	w.WriteHeader(http.StatusNotFound)
	return nil
}

// provisionalDataBroadcastSnapshot reconstructs a data-broadcast snapshot from
// persisted PMT/DII sections without allocating a tuner. It reads the stores
// directly instead of going through the stream manager, which only manages
// sessions.
func provisionalDataBroadcastSnapshot(h *Handler, allowCache apigen.OptInt, channelType, channelID string, serviceID uint16) (bml.Snapshot, int64, bool) {
	if !shouldAllowCache(allowCache) {
		return bml.Snapshot{}, 0, false
	}
	if h.bmlSnapshotStore == nil {
		return bml.Snapshot{}, 0, false
	}
	persisted, found := h.bmlSnapshotStore.GetSnapshot(channelType, channelID, serviceID)
	if !found {
		return bml.Snapshot{}, 0, false
	}
	existence, _ := h.bmlStore.(bml.ModuleExistenceStore)
	return bml.RestoreSnapshot(channelType, channelID, persisted, existence), persisted.StoredAt * 1000, true
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
	manifest := apiDataBroadcastModuleManifest(params.ID, module, resources)
	return writeDataBroadcastJSON(w, &manifest)
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
	if errors.Is(err, resource.ErrModuleResourceLimit) {
		w.WriteHeader(http.StatusInsufficientStorage)
		return nil
	}
	if errors.Is(err, resource.ErrMalformedModule) || errors.Is(err, resource.ErrUnsupportedModuleCompression) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return nil
	}
	return err
}

func dataBroadcastModuleResources(ctx context.Context, h *Handler, serviceItemID int64, module bml.Module) ([]resource.ModuleResource, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(serviceItemID, 10))
	if err != nil {
		return nil, err
	}
	if service != nil {
		if store, ok := h.bmlStore.(cache.DecodedModuleStore); ok {
			if resources, found := store.GetDecodedResources(bml.ModuleVersionKey{
				ChannelType: service.ChannelType, ChannelID: service.ChannelId, ServiceID: service.Key.ServiceID,
				ComponentTag: module.ComponentTag, DownloadID: module.DownloadID, ModuleID: module.ModuleID, Version: module.Version,
			}); found {
				return resources, nil
			}
		}
	}
	return resource.DecodeModuleResources(module)
}

func dataBroadcastVersionModule(ctx context.Context, h *Handler, serviceItemID int64, componentTag byte, downloadID uint32, moduleID uint16, version byte) (bml.Module, int, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(serviceItemID, 10))
	if err != nil {
		return bml.Module{}, 0, err
	}
	if service == nil {
		return bml.Module{}, http.StatusNotFound, nil
	}
	if session, ok := h.streamManager.GetExisting(service.ChannelType, service.ChannelId); ok {
		bmlSession, ok := session.(stream.BMLSource)
		if !ok {
			return bml.Module{}, http.StatusNotFound, nil
		}
		module, found := bmlSession.DataBroadcastModuleVersion(service.Key.ServiceID, componentTag, downloadID, moduleID, version)
		if found {
			return module, 0, nil
		}
		if announced, rejected := announcedModuleVersion(bmlSession.DataBroadcastSnapshot(service.Key.ServiceID), componentTag, downloadID, moduleID, version); rejected {
			return bml.Module{}, http.StatusInsufficientStorage, nil
		} else if announced {
			return bml.Module{}, http.StatusTooEarly, nil
		}
	}
	// DataBroadcastCachedModule resolves a completed immutable module without
	// allocating a tuner or requiring its original channel session to still
	// exist.
	if h.bmlStore != nil {
		if cached, found := h.bmlStore.GetVersion(bml.ModuleVersionKey{
			ChannelType: service.ChannelType, ChannelID: service.ChannelId, ServiceID: service.Key.ServiceID,
			ComponentTag: componentTag, DownloadID: downloadID, ModuleID: moduleID, Version: version,
		}); found {
			return bml.CompletedModule(componentTag, cached), 0, nil
		}
	}
	if store, ok := h.bmlStore.(bml.EvictedModuleStore); ok && store.WasEvicted(bml.ModuleVersionKey{
		ChannelType: service.ChannelType, ChannelID: service.ChannelId, ServiceID: service.Key.ServiceID,
		ComponentTag: componentTag, DownloadID: downloadID, ModuleID: moduleID, Version: version,
	}) {
		return bml.Module{}, http.StatusGone, nil
	}
	return bml.Module{}, http.StatusNotFound, nil
}

func announcedModuleVersion(snapshot bml.Snapshot, componentTag byte, downloadID uint32, moduleID uint16, version byte) (bool, bool) {
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
