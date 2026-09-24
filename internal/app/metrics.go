package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	"go.opentelemetry.io/otel/metric"
)

func registerRuntimeMetrics(
	provider metric.MeterProvider,
	streams *stream.Manager,
	tuners *tuner.Manager,
	jobs *job.Manager,
	programs *program.Manager,
	services *service.Manager,
	events *event.Hub,
	logs *observability.LogStore,
	epgStaleAfter int64,
) {
	if provider == nil {
		return
	}
	meter := observability.Meter(provider)

	streamSessions := observability.NewInt64ObservableGauge(meter, observability.MetricStreamSessionsActive)
	tunerDevices := observability.NewInt64ObservableGauge(meter, observability.MetricTunerDevices)
	tunerUsers := observability.NewInt64ObservableGauge(meter, observability.MetricTunerUsers)
	jobCount := observability.NewInt64ObservableGauge(meter, observability.MetricJobCount)
	epgPrograms := observability.NewInt64ObservableGauge(meter, observability.MetricEPGProgramsStored)
	epgStale := observability.NewInt64ObservableGauge(meter, observability.MetricEPGServicesStale)
	epgFailed := observability.NewInt64ObservableGauge(meter, observability.MetricEPGServicesFailed)
	tunerProcessUptime := observability.NewInt64ObservableGauge(meter, observability.MetricTunerProcessUptime, metric.WithUnit("s"))
	eventSubscribers := observability.NewInt64ObservableGauge(meter, observability.MetricEventsSubscribers)
	logSubscribers := observability.NewInt64ObservableGauge(meter, observability.MetricLogsSubscribers)

	for _, instrument := range []metric.Int64ObservableGauge{
		streamSessions, tunerDevices, tunerUsers, jobCount, epgPrograms,
		epgStale, epgFailed, tunerProcessUptime, eventSubscribers, logSubscribers,
	} {
		if instrument == nil {
			return
		}
	}

	_, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		observer.ObserveInt64(streamSessions, int64(streams.ActiveSessionCount()))
		observeTunerMetrics(observer, tunerDevices, tunerUsers, tuners.Statuses())
		observeTunerProcessUptime(observer, tunerProcessUptime, tuners.ProcessUptimes())
		observeJobMetrics(observer, jobCount, jobs.GetJobs(), jobs.GetJobSchedules())
		observeEPGMetrics(ctx, observer, epgPrograms, epgStale, epgFailed, programs, services, epgStaleAfter)
		eventSubscriberCount := 0
		if events != nil {
			eventSubscriberCount = events.SubscriberCount()
		}
		observer.ObserveInt64(eventSubscribers, int64(eventSubscriberCount))
		logSubscriberCount := 0
		if logs != nil {
			logSubscriberCount = logs.SubscriberCount()
		}
		observer.ObserveInt64(logSubscribers, int64(logSubscriberCount))
		return nil
	}, streamSessions, tunerDevices, tunerUsers, jobCount, epgPrograms, epgStale, epgFailed, tunerProcessUptime, eventSubscribers, logSubscribers)
	if err != nil {
		slog.Warn("failed to register runtime metrics callback", "err", err)
	}
}

func observeTunerMetrics(observer metric.Observer, devices, users metric.Int64ObservableGauge, statuses []tuner.Status) {
	counts := map[string]int64{
		"available": 0,
		"free":      0,
		"using":     0,
		"fault":     0,
	}
	var userCount int64
	for _, status := range statuses {
		if status.IsAvailable {
			counts["available"]++
		}
		if status.IsFree {
			counts["free"]++
		}
		if status.IsUsing {
			counts["using"]++
		}
		if status.IsFault {
			counts["fault"]++
		}
		userCount += int64(len(status.Users))
	}
	for state, count := range counts {
		observer.ObserveInt64(devices, count, metric.WithAttributes(observability.AttrState.String(state)))
	}
	observer.ObserveInt64(users, userCount)
}

func observeTunerProcessUptime(observer metric.Observer, instrument metric.Int64ObservableGauge, uptimes []tuner.ProcessUptime) {
	if len(uptimes) == 0 {
		observer.ObserveInt64(instrument, 0)
		return
	}
	for _, uptime := range uptimes {
		observer.ObserveInt64(instrument, uptime.UptimeSeconds, metric.WithAttributes(
			observability.AttrTunerIndex.Int(uptime.Index),
			observability.AttrTunerName.String(uptime.Name),
			observability.AttrChannelType.String(uptime.ChannelType),
			observability.AttrChannelID.String(uptime.ChannelID),
		))
	}
}

func observeJobMetrics(observer metric.Observer, instrument metric.Int64ObservableGauge, jobs []*job.Job, schedules []job.ScheduleInfo) {
	counts := make(map[jobMetricKey]int64)
	for _, schedule := range schedules {
		for _, status := range []job.JobStatus{job.StatusQueued, job.StatusStandby, job.StatusRunning, job.StatusFinished} {
			counts[jobMetricKey{key: schedule.JobKey, status: string(status)}] = 0
		}
	}
	for _, item := range jobs {
		counts[jobMetricKey{key: item.Key, status: string(item.Status)}]++
	}
	for key, count := range counts {
		observer.ObserveInt64(instrument, count, metric.WithAttributes(
			observability.AttrJobKey.String(key.key),
			observability.AttrJobStatus.String(key.status),
		))
	}
}

func observeEPGMetrics(
	ctx context.Context,
	observer metric.Observer,
	programsInstrument, staleInstrument, failedInstrument metric.Int64ObservableGauge,
	programs *program.Manager,
	services *service.Manager,
	epgStaleAfter int64,
) {
	count, err := programs.Count(ctx)
	if err != nil {
		slog.Debug("failed to observe stored EPG programs metric", "err", err)
	} else {
		observer.ObserveInt64(programsInstrument, int64(count))
	}

	stale, failed, _, err := services.EPGSummary(ctx, epgStaleAfter, time.Now().UnixMilli())
	if err != nil {
		slog.Debug("failed to observe EPG service metrics", "err", err)
		return
	}
	observer.ObserveInt64(staleInstrument, int64(stale))
	observer.ObserveInt64(failedInstrument, int64(failed))
}

type jobMetricKey struct {
	key    string
	status string
}
