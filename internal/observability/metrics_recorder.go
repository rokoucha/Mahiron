package observability

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

type epgMetricSourceContextKey struct{}

func ContextWithEPGMetricSource(ctx context.Context, source string) context.Context {
	if source == "" {
		return ctx
	}
	return context.WithValue(ctx, epgMetricSourceContextKey{}, source)
}

func EPGMetricSource(ctx context.Context) string {
	source, _ := ctx.Value(epgMetricSourceContextKey{}).(string)
	return source
}

func RecordJobRun(ctx context.Context, key, result string, durationMS int64) {
	attrs := metric.WithAttributes(AttrJobKey.String(key), AttrJobResult.String(result))
	if jobMetrics.runs != nil {
		jobMetrics.runs.Add(ctx, 1, attrs)
	}
	if jobMetrics.duration != nil && durationMS >= 0 {
		jobMetrics.duration.Record(ctx, durationMS, attrs)
	}
}

func RecordStreamSessionStart(ctx context.Context, channelType, routeType, source, result string) {
	if jobMetrics.streamSessionStarts == nil {
		return
	}
	jobMetrics.streamSessionStarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrRouteType.String(routeType),
		AttrSource.String(source),
		AttrResult.String(result),
	))
}

func RecordStreamSessionDuration(ctx context.Context, channelType, routeType, source string, durationMS int64) {
	if jobMetrics.streamSessionDuration == nil || durationMS < 0 {
		return
	}
	jobMetrics.streamSessionDuration.Record(ctx, durationMS, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrRouteType.String(routeType),
		AttrSource.String(source),
	))
}

func RecordTunerAcquire(ctx context.Context, channelType, result string, wait bool, durationMS int64) {
	attrs := metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrResult.String(result),
		AttrWait.Bool(wait),
	)
	if jobMetrics.tunerAcquireRequests != nil {
		jobMetrics.tunerAcquireRequests.Add(ctx, 1, attrs)
	}
	if jobMetrics.tunerAcquireDuration != nil && durationMS >= 0 {
		jobMetrics.tunerAcquireDuration.Record(ctx, durationMS, attrs)
	}
}

func RecordStreamPacket(ctx context.Context, channelType, channelID string, bytes int64) {
	attrs := metric.WithAttributes(AttrChannelType.String(channelType), AttrChannelID.String(channelID))
	if jobMetrics.streamPackets != nil {
		jobMetrics.streamPackets.Add(ctx, 1, attrs)
	}
	if jobMetrics.streamBytes != nil && bytes > 0 {
		jobMetrics.streamBytes.Add(ctx, bytes, attrs)
	}
}

func RecordStreamPacketError(ctx context.Context, channelType, channelID, result string) {
	if jobMetrics.streamPacketErrors == nil {
		return
	}
	jobMetrics.streamPacketErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordStreamContinuityCounterError(ctx context.Context, channelType, channelID string) {
	if jobMetrics.streamContinuityErrors == nil {
		return
	}
	jobMetrics.streamContinuityErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
	))
}

func RecordStreamSubscriberError(ctx context.Context, channelType, result string) {
	if jobMetrics.streamSubscriberErrors == nil {
		return
	}
	jobMetrics.streamSubscriberErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrResult.String(result),
	))
}

func RecordStreamSubscriberOverflow(ctx context.Context, channelType, result string) {
	RecordStreamSubscriberError(ctx, channelType, result)
	if jobMetrics.streamSubscriberOverflow == nil {
		return
	}
	jobMetrics.streamSubscriberOverflow.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrResult.String(result),
	))
}

func RecordTunerProcessStart(ctx context.Context, channelType, channelID, result string) {
	if jobMetrics.tunerProcessStarts == nil {
		return
	}
	jobMetrics.tunerProcessStarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordTunerProcessExit(ctx context.Context, channelType, channelID, result string) {
	if jobMetrics.tunerProcessExits == nil {
		return
	}
	jobMetrics.tunerProcessExits.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordTunerProcessRestartAttempt(ctx context.Context, channelType, channelID string) {
	if jobMetrics.tunerProcessRestarts == nil {
		return
	}
	jobMetrics.tunerProcessRestarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
	))
}

func RecordRemoteOperation(ctx context.Context, operation, result string, durationMS int64) {
	attrs := metric.WithAttributes(AttrOperation.String(operation), AttrResult.String(result))
	if jobMetrics.remoteRequests != nil {
		jobMetrics.remoteRequests.Add(ctx, 1, attrs)
	}
	if jobMetrics.remoteDuration != nil && durationMS >= 0 {
		jobMetrics.remoteDuration.Record(ctx, durationMS, attrs)
	}
	if jobMetrics.remoteErrors != nil && result != "success" {
		jobMetrics.remoteErrors.Add(ctx, 1, attrs)
	}
}

func RecordDBOperation(ctx context.Context, operation string, durationMS int64, err error) {
	attrs := metric.WithAttributes(AttrOperation.String(operation))
	if jobMetrics.dbOperationDuration != nil && durationMS >= 0 {
		jobMetrics.dbOperationDuration.Record(ctx, durationMS, attrs)
	}
	if jobMetrics.dbOperationErrors != nil && err != nil {
		jobMetrics.dbOperationErrors.Add(ctx, 1, attrs)
	}
}

func RecordEventPublished(ctx context.Context, resource, typ string) {
	if jobMetrics.eventsPublished == nil {
		return
	}
	jobMetrics.eventsPublished.Add(ctx, 1, metric.WithAttributes(
		AttrEventResource.String(resource),
		AttrEventType.String(typ),
	))
}

func RecordEventDropped(ctx context.Context) {
	if jobMetrics.eventsDropped == nil {
		return
	}
	jobMetrics.eventsDropped.Add(ctx, 1)
}

func RecordLogDropped(ctx context.Context) {
	if jobMetrics.logsDropped == nil {
		return
	}
	jobMetrics.logsDropped.Add(ctx, 1)
}

func RecordEPGProgramsUpserted(ctx context.Context, source, result string, count int64) {
	if jobMetrics.epgProgramsUpserted == nil || source == "" || count <= 0 {
		return
	}
	jobMetrics.epgProgramsUpserted.Add(ctx, count, metric.WithAttributes(
		AttrSource.String(source),
		AttrResult.String(result),
	))
}

func RecordEPGProgramsDeleted(ctx context.Context, source, result string, count int64) {
	if jobMetrics.epgProgramsDeleted == nil || source == "" || count <= 0 {
		return
	}
	jobMetrics.epgProgramsDeleted.Add(ctx, count, metric.WithAttributes(
		AttrSource.String(source),
		AttrResult.String(result),
	))
}

func RecordEPGServiceUpdateError(ctx context.Context, source, result string) {
	if jobMetrics.epgServiceUpdateErrors == nil || source == "" {
		return
	}
	jobMetrics.epgServiceUpdateErrors.Add(ctx, 1, metric.WithAttributes(
		AttrSource.String(source),
		AttrResult.String(result),
	))
}
