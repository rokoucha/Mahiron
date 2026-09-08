package api

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

const epgGatherJobKeyPrefix = "epg-gather:nid:"

func GetStatus(ctx context.Context, h *Handler) (apigen.GetStatusRes, error) {
	now := time.Now()
	result := &apigen.Status{
		Time:    apigen.NewOptInt(int(now.UnixMilli())),
		Version: apigen.NewOptString(currentVersion),
		Process: apigen.NewOptStatusProcess(apiStatusProcess()),
	}

	epg := buildStatusEpg(ctx, h, now)
	result.Epg = apigen.NewOptStatusEpg(*epg)

	if streamCount, ok := buildStatusStreamCount(h); ok {
		result.StreamCount = apigen.NewOptStatusStreamCount(streamCount)
	}

	return result, nil
}

func apiStatusProcess() apigen.StatusProcess {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return apigen.StatusProcess{
		Arch:     apigen.NewOptString(runtime.GOARCH),
		Platform: apigen.NewOptString(runtime.GOOS),
		Pid:      apigen.NewOptInt(os.Getpid()),
		MemoryUsage: apigen.NewOptStatusProcessMemoryUsage(apigen.StatusProcessMemoryUsage{
			Rss:       apigen.NewOptInt(int(processRSS(mem.Sys))),
			HeapTotal: apigen.NewOptInt(int(mem.HeapSys)),
			HeapUsed:  apigen.NewOptInt(int(mem.HeapAlloc)),
		}),
	}
}

// processRSS reports how much memory the process is actually holding.
// runtime.MemStats.Sys counts every byte the runtime has ever taken from the
// OS, including what the scavenger has since handed back, so after one large
// transient allocation it keeps reporting that peak forever — 478 MB against a
// process resident in 118 MB, in one case — which makes the figure useless for
// judging whether the process is near its limit. Linux publishes the real
// number in /proc/self/statm; everywhere else Sys is the closest thing there
// is.
func processRSS(fallback uint64) uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return fallback
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return fallback
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return fallback
	}
	return pages * uint64(os.Getpagesize())
}

func buildStatusEpg(ctx context.Context, h *Handler, now time.Time) *apigen.StatusEpg {
	epg := &apigen.StatusEpg{}

	if h.jobManager != nil {
		for _, key := range h.jobManager.GetActiveJobKeysByPrefix(epgGatherJobKeyPrefix) {
			nidStr := strings.TrimPrefix(key, epgGatherJobKeyPrefix)
			nid, err := strconv.ParseUint(nidStr, 10, 16)
			if err != nil {
				continue
			}
			epg.GatheringNetworks = append(epg.GatheringNetworks, apigen.NetworkId(nid))
		}
	}

	if h.programManager != nil {
		if count, err := h.programManager.Count(ctx); err == nil {
			epg.StoredEvents = apigen.NewOptInt(count)
		}
	}

	if h.serviceManager != nil {
		stale, failed, lastSuccess, err := h.serviceManager.EPGSummary(ctx, h.epgStaleAfter, now.UnixMilli())
		if err == nil {
			epg.StaleServices = apigen.NewOptInt(stale)
			epg.FailedServices = apigen.NewOptInt(failed)
			if lastSuccess != nil {
				epg.LastUpdatedAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*lastSuccess))
			}
		}
	}

	return epg
}

func buildStatusStreamCount(h *Handler) (apigen.StatusStreamCount, bool) {
	var streamCount apigen.StatusStreamCount
	hasValue := false

	if h.tunerManager != nil {
		using := 0
		for _, status := range h.tunerManager.Statuses() {
			if status.IsUsing {
				using++
			}
		}
		streamCount.TunerDevice = apigen.NewOptInt(using)
		hasValue = true
	}

	if h.streamManager != nil {
		count := h.streamManager.ActiveSessionCount()
		streamCount.TsFilter = apigen.NewOptInt(count)
		streamCount.Decoder = apigen.NewOptInt(count)
		hasValue = true
	}

	return streamCount, hasValue
}
