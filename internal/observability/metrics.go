package observability

import (
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	MetricStreamSessionsActive     = "mahiron.stream.sessions.active"
	MetricTunerDevices             = "mahiron.tuner.devices"
	MetricTunerUsers               = "mahiron.tuner.users"
	MetricJobs                     = "mahiron.jobs"
	MetricEPGProgramsStored        = "mahiron.epg.programs.stored"
	MetricEPGServicesStale         = "mahiron.epg.services.stale"
	MetricEPGServicesFailed        = "mahiron.epg.services.failed"
	MetricJobRuns                  = "mahiron.job.runs"
	MetricJobDuration              = "mahiron.job.duration"
	MetricStreamSessionStarts      = "mahiron.stream.session.starts"
	MetricStreamSessionDuration    = "mahiron.stream.session.duration"
	MetricStreamBytes              = "mahiron.stream.bytes"
	MetricStreamPackets            = "mahiron.stream.packets"
	MetricStreamPacketErrors       = "mahiron.stream.packet.errors"
	MetricStreamContinuityErrors   = "mahiron.stream.continuity_counter.errors"
	MetricTunerAcquireRequests     = "mahiron.tuner.acquire.requests"
	MetricTunerAcquireDuration     = "mahiron.tuner.acquire.duration"
	MetricTunerProcessStarts       = "mahiron.tuner.process.starts"
	MetricTunerProcessExits        = "mahiron.tuner.process.exits"
	MetricTunerProcessRestarts     = "mahiron.tuner.process.restart_attempts"
	MetricTunerProcessUptime       = "mahiron.tuner.process.uptime"
	MetricRemoteRequests           = "mahiron.remote.requests"
	MetricRemoteDuration           = "mahiron.remote.duration"
	MetricRemoteErrors             = "mahiron.remote.errors"
	MetricDBOperationDuration      = "mahiron.db.operation.duration"
	MetricDBOperationErrors        = "mahiron.db.operation.errors"
	MetricEventsSubscribers        = "mahiron.events.subscribers"
	MetricLogsSubscribers          = "mahiron.logs.subscribers"
	MetricEventsPublished          = "mahiron.events.published"
	MetricStreamSubscriberErrors   = "mahiron.stream.subscriber.errors"
	MetricStreamSubscriberOverflow = "mahiron.stream.subscriber.overflow"
	MetricEventsDropped            = "mahiron.events.dropped"
	MetricLogsDropped              = "mahiron.logs.dropped"
	MetricEPGProgramsUpserted      = "mahiron.epg.programs.upserted"
	MetricEPGProgramsDeleted       = "mahiron.epg.programs.deleted"
	MetricEPGServiceUpdateErrors   = "mahiron.epg.service.update.errors"
)

const (
	AttrEventResource attribute.Key = "event.resource"
	AttrEventType     attribute.Key = "event.type"
	AttrJobResult     attribute.Key = "job.result"
	AttrOperation     attribute.Key = "operation"
	AttrResult        attribute.Key = "result"
	AttrSource        attribute.Key = "source"
	AttrState         attribute.Key = "state"
	AttrTunerIndex    attribute.Key = "tuner.index"
	AttrTunerName     attribute.Key = "tuner.name"
)

var jobMetrics struct {
	runs                     metric.Int64Counter
	duration                 metric.Int64Histogram
	streamSessionStarts      metric.Int64Counter
	streamSessionDuration    metric.Int64Histogram
	streamBytes              metric.Int64Counter
	streamPackets            metric.Int64Counter
	streamPacketErrors       metric.Int64Counter
	streamContinuityErrors   metric.Int64Counter
	tunerAcquireRequests     metric.Int64Counter
	tunerAcquireDuration     metric.Int64Histogram
	tunerProcessStarts       metric.Int64Counter
	tunerProcessExits        metric.Int64Counter
	tunerProcessRestarts     metric.Int64Counter
	remoteRequests           metric.Int64Counter
	remoteDuration           metric.Int64Histogram
	remoteErrors             metric.Int64Counter
	dbOperationDuration      metric.Int64Histogram
	dbOperationErrors        metric.Int64Counter
	eventsPublished          metric.Int64Counter
	streamSubscriberErrors   metric.Int64Counter
	streamSubscriberOverflow metric.Int64Counter
	eventsDropped            metric.Int64Counter
	logsDropped              metric.Int64Counter
	epgProgramsUpserted      metric.Int64Counter
	epgProgramsDeleted       metric.Int64Counter
	epgServiceUpdateErrors   metric.Int64Counter
}

func initMetrics(provider metric.MeterProvider) {
	meter := provider.Meter(instrumentationName)
	runs, err := meter.Int64Counter(MetricJobRuns)
	if err != nil {
		slog.Warn("failed to create job run metric", "err", err)
	}
	duration, err := meter.Int64Histogram(MetricJobDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create job duration metric", "err", err)
	}
	streamSessionStarts, err := meter.Int64Counter(MetricStreamSessionStarts)
	if err != nil {
		slog.Warn("failed to create stream session starts metric", "err", err)
	}
	streamSessionDuration, err := meter.Int64Histogram(MetricStreamSessionDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create stream session duration metric", "err", err)
	}
	tunerAcquireRequests, err := meter.Int64Counter(MetricTunerAcquireRequests)
	if err != nil {
		slog.Warn("failed to create tuner acquire requests metric", "err", err)
	}
	tunerAcquireDuration, err := meter.Int64Histogram(MetricTunerAcquireDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create tuner acquire duration metric", "err", err)
	}
	streamBytes, err := meter.Int64Counter(MetricStreamBytes, metric.WithUnit("By"))
	if err != nil {
		slog.Warn("failed to create stream bytes metric", "err", err)
	}
	streamPackets, err := meter.Int64Counter(MetricStreamPackets)
	if err != nil {
		slog.Warn("failed to create stream packets metric", "err", err)
	}
	streamPacketErrors, err := meter.Int64Counter(MetricStreamPacketErrors)
	if err != nil {
		slog.Warn("failed to create stream packet errors metric", "err", err)
	}
	streamContinuityErrors, err := meter.Int64Counter(MetricStreamContinuityErrors)
	if err != nil {
		slog.Warn("failed to create stream continuity counter errors metric", "err", err)
	}
	tunerProcessStarts, err := meter.Int64Counter(MetricTunerProcessStarts)
	if err != nil {
		slog.Warn("failed to create tuner process starts metric", "err", err)
	}
	tunerProcessExits, err := meter.Int64Counter(MetricTunerProcessExits)
	if err != nil {
		slog.Warn("failed to create tuner process exits metric", "err", err)
	}
	tunerProcessRestarts, err := meter.Int64Counter(MetricTunerProcessRestarts)
	if err != nil {
		slog.Warn("failed to create tuner process restart attempts metric", "err", err)
	}
	remoteRequests, err := meter.Int64Counter(MetricRemoteRequests)
	if err != nil {
		slog.Warn("failed to create remote requests metric", "err", err)
	}
	remoteDuration, err := meter.Int64Histogram(MetricRemoteDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create remote duration metric", "err", err)
	}
	remoteErrors, err := meter.Int64Counter(MetricRemoteErrors)
	if err != nil {
		slog.Warn("failed to create remote errors metric", "err", err)
	}
	dbOperationDuration, err := meter.Int64Histogram(MetricDBOperationDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create DB operation duration metric", "err", err)
	}
	dbOperationErrors, err := meter.Int64Counter(MetricDBOperationErrors)
	if err != nil {
		slog.Warn("failed to create DB operation errors metric", "err", err)
	}
	eventsPublished, err := meter.Int64Counter(MetricEventsPublished)
	if err != nil {
		slog.Warn("failed to create events published metric", "err", err)
	}
	streamSubscriberErrors, err := meter.Int64Counter(MetricStreamSubscriberErrors)
	if err != nil {
		slog.Warn("failed to create stream subscriber errors metric", "err", err)
	}
	streamSubscriberOverflow, err := meter.Int64Counter(MetricStreamSubscriberOverflow)
	if err != nil {
		slog.Warn("failed to create stream subscriber overflow metric", "err", err)
	}
	eventsDropped, err := meter.Int64Counter(MetricEventsDropped)
	if err != nil {
		slog.Warn("failed to create events dropped metric", "err", err)
	}
	logsDropped, err := meter.Int64Counter(MetricLogsDropped)
	if err != nil {
		slog.Warn("failed to create logs dropped metric", "err", err)
	}
	epgProgramsUpserted, err := meter.Int64Counter(MetricEPGProgramsUpserted)
	if err != nil {
		slog.Warn("failed to create EPG programs upserted metric", "err", err)
	}
	epgProgramsDeleted, err := meter.Int64Counter(MetricEPGProgramsDeleted)
	if err != nil {
		slog.Warn("failed to create EPG programs deleted metric", "err", err)
	}
	epgServiceUpdateErrors, err := meter.Int64Counter(MetricEPGServiceUpdateErrors)
	if err != nil {
		slog.Warn("failed to create EPG service update errors metric", "err", err)
	}
	jobMetrics.runs = runs
	jobMetrics.duration = duration
	jobMetrics.streamSessionStarts = streamSessionStarts
	jobMetrics.streamSessionDuration = streamSessionDuration
	jobMetrics.streamBytes = streamBytes
	jobMetrics.streamPackets = streamPackets
	jobMetrics.streamPacketErrors = streamPacketErrors
	jobMetrics.streamContinuityErrors = streamContinuityErrors
	jobMetrics.tunerAcquireRequests = tunerAcquireRequests
	jobMetrics.tunerAcquireDuration = tunerAcquireDuration
	jobMetrics.tunerProcessStarts = tunerProcessStarts
	jobMetrics.tunerProcessExits = tunerProcessExits
	jobMetrics.tunerProcessRestarts = tunerProcessRestarts
	jobMetrics.remoteRequests = remoteRequests
	jobMetrics.remoteDuration = remoteDuration
	jobMetrics.remoteErrors = remoteErrors
	jobMetrics.dbOperationDuration = dbOperationDuration
	jobMetrics.dbOperationErrors = dbOperationErrors
	jobMetrics.eventsPublished = eventsPublished
	jobMetrics.streamSubscriberErrors = streamSubscriberErrors
	jobMetrics.streamSubscriberOverflow = streamSubscriberOverflow
	jobMetrics.eventsDropped = eventsDropped
	jobMetrics.logsDropped = logsDropped
	jobMetrics.epgProgramsUpserted = epgProgramsUpserted
	jobMetrics.epgProgramsDeleted = epgProgramsDeleted
	jobMetrics.epgServiceUpdateErrors = epgServiceUpdateErrors
}
