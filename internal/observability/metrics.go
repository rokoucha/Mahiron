package observability

import (
	"log/slog"

	"go.opentelemetry.io/otel/metric"
)

const (
	MetricStreamSessionsActive        = "mahiron.stream.session.active"
	MetricTunerDevices                = "mahiron.tuner.devices"
	MetricTunerUsers                  = "mahiron.tuner.users"
	MetricJobCount                    = "mahiron.job.count"
	MetricEPGProgramsStored           = "mahiron.epg.program.stored"
	MetricEPGServicesStale            = "mahiron.epg.service.stale"
	MetricEPGServicesFailed           = "mahiron.epg.service.failed"
	MetricJobRuns                     = "mahiron.job.runs"
	MetricJobDuration                 = "mahiron.job.duration"
	MetricJobItems                    = "mahiron.job.items"
	MetricStreamSessionStarts         = "mahiron.stream.session.starts"
	MetricStreamSessionDuration       = "mahiron.stream.session.duration"
	MetricStreamBytes                 = "mahiron.stream.bytes"
	MetricStreamPackets               = "mahiron.stream.packets"
	MetricStreamPacketErrors          = "mahiron.stream.packet.errors"
	MetricStreamContinuityErrors      = "mahiron.stream.continuity_counter.errors"
	MetricTunerAcquireRequests        = "mahiron.tuner.acquire.requests"
	MetricTunerAcquireDuration        = "mahiron.tuner.acquire.duration"
	MetricTunerProcessStarts          = "mahiron.tuner.process.starts"
	MetricTunerProcessExits           = "mahiron.tuner.process.exits"
	MetricTunerProcessRestarts        = "mahiron.tuner.process.restart_attempts"
	MetricTunerProcessUptime          = "mahiron.tuner.process.uptime"
	MetricRemoteRequests              = "mahiron.remote.requests"
	MetricRemoteDuration              = "mahiron.remote.duration"
	MetricRemoteErrors                = "mahiron.remote.errors"
	MetricDBOperationDuration         = "mahiron.db.operation.duration"
	MetricDBOperationErrors           = "mahiron.db.operation.errors"
	MetricEventsSubscribers           = "mahiron.events.subscribers"
	MetricLogsSubscribers             = "mahiron.logs.subscribers"
	MetricEventsPublished             = "mahiron.events.published"
	MetricStreamSubscriberErrors      = "mahiron.stream.subscriber.errors"
	MetricStreamSubscriberOverflow    = "mahiron.stream.subscriber.overflow"
	MetricStreamFanoutDroppedChunks   = "mahiron.stream.fanout.dropped_chunks"
	MetricStreamFanoutDroppedBytes    = "mahiron.stream.fanout.dropped_bytes"
	MetricStreamFanoutQueueDepth      = "mahiron.stream.fanout.queue_depth"
	MetricEventsDropped               = "mahiron.events.dropped"
	MetricLogsDropped                 = "mahiron.logs.dropped"
	MetricEPGProgramsUpserted         = "mahiron.epg.program.upserted"
	MetricEPGProgramsDeleted          = "mahiron.epg.program.deleted"
	MetricEPGServiceUpdateErrors      = "mahiron.epg.service.update_errors"
	MetricDataBroadcastCarouselEvents = "mahiron.data_broadcast.carousel.events"
	MetricDataBroadcastModuleDuration = "mahiron.data_broadcast.module.duration"
)

// instrumentSet holds the package-level instruments shared by the Record*
// functions in recorders.go, grouped by domain.
type instrumentSet struct {
	// job
	jobRuns     metric.Int64Counter
	jobDuration metric.Int64Histogram
	jobItems    metric.Int64Counter

	// stream
	streamSessionStarts         metric.Int64Counter
	streamSessionDuration       metric.Int64Histogram
	streamBytes                 metric.Int64Counter
	streamPackets               metric.Int64Counter
	streamPacketErrors          metric.Int64Counter
	streamContinuityErrors      metric.Int64Counter
	streamSubscriberErrors      metric.Int64Counter
	streamSubscriberOverflow    metric.Int64Counter
	streamFanoutDroppedChunks   metric.Int64Counter
	streamFanoutDroppedBytes    metric.Int64Counter
	streamFanoutQueueDepth      metric.Int64UpDownCounter
	dataBroadcastCarouselEvents metric.Int64Counter
	dataBroadcastModuleDuration metric.Int64Histogram

	// tuner
	tunerAcquireRequests metric.Int64Counter
	tunerAcquireDuration metric.Int64Histogram
	tunerProcessStarts   metric.Int64Counter
	tunerProcessExits    metric.Int64Counter
	tunerProcessRestarts metric.Int64Counter

	// remote
	remoteRequests metric.Int64Counter
	remoteDuration metric.Int64Histogram
	remoteErrors   metric.Int64Counter

	// db
	dbOperationDuration metric.Int64Histogram
	dbOperationErrors   metric.Int64Counter

	// event
	eventsPublished metric.Int64Counter
	eventsDropped   metric.Int64Counter

	// log
	logsDropped metric.Int64Counter

	// epg
	epgProgramsUpserted    metric.Int64Counter
	epgProgramsDeleted     metric.Int64Counter
	epgServiceUpdateErrors metric.Int64Counter
}

var instruments instrumentSet

func initMetrics(provider metric.MeterProvider) {
	meter := provider.Meter(instrumentationName)

	instruments = instrumentSet{
		jobRuns:     newInt64Counter(meter, MetricJobRuns),
		jobDuration: newInt64Histogram(meter, MetricJobDuration, metric.WithUnit("ms")),
		jobItems:    newInt64Counter(meter, MetricJobItems),

		streamSessionStarts:         newInt64Counter(meter, MetricStreamSessionStarts),
		streamSessionDuration:       newInt64Histogram(meter, MetricStreamSessionDuration, metric.WithUnit("ms")),
		streamBytes:                 newInt64Counter(meter, MetricStreamBytes, metric.WithUnit("By")),
		streamPackets:               newInt64Counter(meter, MetricStreamPackets),
		streamPacketErrors:          newInt64Counter(meter, MetricStreamPacketErrors),
		streamContinuityErrors:      newInt64Counter(meter, MetricStreamContinuityErrors),
		streamSubscriberErrors:      newInt64Counter(meter, MetricStreamSubscriberErrors),
		streamSubscriberOverflow:    newInt64Counter(meter, MetricStreamSubscriberOverflow),
		streamFanoutDroppedChunks:   newInt64Counter(meter, MetricStreamFanoutDroppedChunks),
		streamFanoutDroppedBytes:    newInt64Counter(meter, MetricStreamFanoutDroppedBytes, metric.WithUnit("By")),
		streamFanoutQueueDepth:      newInt64UpDownCounter(meter, MetricStreamFanoutQueueDepth),
		dataBroadcastCarouselEvents: newInt64Counter(meter, MetricDataBroadcastCarouselEvents),
		dataBroadcastModuleDuration: newInt64Histogram(meter, MetricDataBroadcastModuleDuration, metric.WithUnit("ms")),

		tunerAcquireRequests: newInt64Counter(meter, MetricTunerAcquireRequests),
		tunerAcquireDuration: newInt64Histogram(meter, MetricTunerAcquireDuration, metric.WithUnit("ms")),
		tunerProcessStarts:   newInt64Counter(meter, MetricTunerProcessStarts),
		tunerProcessExits:    newInt64Counter(meter, MetricTunerProcessExits),
		tunerProcessRestarts: newInt64Counter(meter, MetricTunerProcessRestarts),

		remoteRequests: newInt64Counter(meter, MetricRemoteRequests),
		remoteDuration: newInt64Histogram(meter, MetricRemoteDuration, metric.WithUnit("ms")),
		remoteErrors:   newInt64Counter(meter, MetricRemoteErrors),

		dbOperationDuration: newInt64Histogram(meter, MetricDBOperationDuration, metric.WithUnit("ms")),
		dbOperationErrors:   newInt64Counter(meter, MetricDBOperationErrors),

		eventsPublished: newInt64Counter(meter, MetricEventsPublished),
		eventsDropped:   newInt64Counter(meter, MetricEventsDropped),

		logsDropped: newInt64Counter(meter, MetricLogsDropped),

		epgProgramsUpserted:    newInt64Counter(meter, MetricEPGProgramsUpserted),
		epgProgramsDeleted:     newInt64Counter(meter, MetricEPGProgramsDeleted),
		epgServiceUpdateErrors: newInt64Counter(meter, MetricEPGServiceUpdateErrors),
	}
}

func newInt64Counter(meter metric.Meter, name string, opts ...metric.Int64CounterOption) metric.Int64Counter {
	instrument, err := meter.Int64Counter(name, opts...)
	if err != nil {
		slog.Warn("failed to create metric instrument", "metric", name, "err", err)
	}
	return instrument
}

func newInt64Histogram(meter metric.Meter, name string, opts ...metric.Int64HistogramOption) metric.Int64Histogram {
	instrument, err := meter.Int64Histogram(name, opts...)
	if err != nil {
		slog.Warn("failed to create metric instrument", "metric", name, "err", err)
	}
	return instrument
}

func newInt64UpDownCounter(meter metric.Meter, name string, opts ...metric.Int64UpDownCounterOption) metric.Int64UpDownCounter {
	instrument, err := meter.Int64UpDownCounter(name, opts...)
	if err != nil {
		slog.Warn("failed to create metric instrument", "metric", name, "err", err)
	}
	return instrument
}
