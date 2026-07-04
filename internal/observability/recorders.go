// Record* functions in this file are called from many packages, including
// sites with no request or operation context available (e.g. background
// workers in job/manager.go, event/hub.go, tuner/device.go, and stream
// session teardown paths). Passing context.Background() at those call sites
// is an intentional, accepted convention rather than an oversight — do not
// add context plumbing there solely to satisfy these functions.

package observability

import (
	"context"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/util"
	"go.opentelemetry.io/otel/metric"
)

var epgMetricSourceContextKey util.ContextKey[string]

func ContextWithEPGMetricSource(ctx context.Context, source string) context.Context {
	if source == "" {
		return ctx
	}
	return util.ContextWith(ctx, epgMetricSourceContextKey, source)
}

func EPGMetricSource(ctx context.Context) string {
	source, _ := util.ContextGet(ctx, epgMetricSourceContextKey)
	return source
}

func RecordJobRun(ctx context.Context, key, result string, durationMS int64) {
	attrs := metric.WithAttributes(AttrJobKey.String(key), AttrJobResult.String(result))
	if instruments.jobRuns != nil {
		instruments.jobRuns.Add(ctx, 1, attrs)
	}
	if instruments.jobDuration != nil && durationMS >= 0 {
		instruments.jobDuration.Record(ctx, durationMS, attrs)
	}
}

func RecordJobItems(ctx context.Context, key string, result run.Result) {
	if instruments.jobItems == nil || key == "" {
		return
	}
	status := result.Kind
	if status == "" {
		status = "unknown"
	}
	for countKey, count := range result.Counts {
		if count <= 0 {
			continue
		}
		instruments.jobItems.Add(ctx, int64(count), metric.WithAttributes(
			AttrJobKey.String(key),
			AttrJobResult.String(status),
			AttrJobItemKind.String(countKey),
		))
	}
	byKind := make(map[string]int64)
	for _, item := range result.Items {
		kind := item.Kind
		if kind == "" {
			kind = "item"
		}
		byKind[kind]++
	}
	for kind, count := range byKind {
		instruments.jobItems.Add(ctx, count, metric.WithAttributes(
			AttrJobKey.String(key),
			AttrJobResult.String(status),
			AttrJobItemKind.String(kind),
		))
	}
	if len(result.Warnings) > 0 {
		instruments.jobItems.Add(ctx, int64(len(result.Warnings)), metric.WithAttributes(
			AttrJobKey.String(key),
			AttrJobResult.String(status),
			AttrJobItemKind.String("warnings"),
		))
	}
}

func RecordStreamSessionStart(ctx context.Context, channelType, routeType, source, result string) {
	if instruments.streamSessionStarts == nil {
		return
	}
	instruments.streamSessionStarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrRouteType.String(routeType),
		AttrSource.String(source),
		AttrResult.String(result),
	))
}

func RecordStreamSessionDuration(ctx context.Context, channelType, routeType, source string, durationMS int64) {
	if instruments.streamSessionDuration == nil || durationMS < 0 {
		return
	}
	instruments.streamSessionDuration.Record(ctx, durationMS, metric.WithAttributes(
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
	if instruments.tunerAcquireRequests != nil {
		instruments.tunerAcquireRequests.Add(ctx, 1, attrs)
	}
	if instruments.tunerAcquireDuration != nil && durationMS >= 0 {
		instruments.tunerAcquireDuration.Record(ctx, durationMS, attrs)
	}
}

func RecordStreamPacket(ctx context.Context, channelType, channelID string, bytes int64) {
	RecordStreamPackets(ctx, channelType, channelID, 1, bytes)
}

func RecordStreamPackets(ctx context.Context, channelType, channelID string, packets, bytes int64) {
	if packets <= 0 && bytes <= 0 {
		return
	}
	attrs := metric.WithAttributes(AttrChannelType.String(channelType), AttrChannelID.String(channelID))
	if instruments.streamPackets != nil && packets > 0 {
		instruments.streamPackets.Add(ctx, packets, attrs)
	}
	if instruments.streamBytes != nil && bytes > 0 {
		instruments.streamBytes.Add(ctx, bytes, attrs)
	}
}

func RecordStreamPacketError(ctx context.Context, channelType, channelID, result string) {
	if instruments.streamPacketErrors == nil {
		return
	}
	instruments.streamPacketErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordStreamContinuityCounterError(ctx context.Context, channelType, channelID string) {
	if instruments.streamContinuityErrors == nil {
		return
	}
	instruments.streamContinuityErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
	))
}

func RecordStreamSubscriberError(ctx context.Context, channelType, result string) {
	if instruments.streamSubscriberErrors == nil {
		return
	}
	instruments.streamSubscriberErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrResult.String(result),
	))
}

func RecordStreamSubscriberOverflow(ctx context.Context, channelType, result string) {
	RecordStreamSubscriberError(ctx, channelType, result)
	if instruments.streamSubscriberOverflow == nil {
		return
	}
	instruments.streamSubscriberOverflow.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrResult.String(result),
	))
}

func RecordTunerProcessStart(ctx context.Context, channelType, channelID, result string) {
	if instruments.tunerProcessStarts == nil {
		return
	}
	instruments.tunerProcessStarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordTunerProcessExit(ctx context.Context, channelType, channelID, result string) {
	if instruments.tunerProcessExits == nil {
		return
	}
	instruments.tunerProcessExits.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordTunerProcessRestartAttempt(ctx context.Context, channelType, channelID string) {
	if instruments.tunerProcessRestarts == nil {
		return
	}
	instruments.tunerProcessRestarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
	))
}

func RecordRemoteOperation(ctx context.Context, operation, result string, durationMS int64) {
	attrs := metric.WithAttributes(AttrOperation.String(operation), AttrResult.String(result))
	if instruments.remoteRequests != nil {
		instruments.remoteRequests.Add(ctx, 1, attrs)
	}
	if instruments.remoteDuration != nil && durationMS >= 0 {
		instruments.remoteDuration.Record(ctx, durationMS, attrs)
	}
	if instruments.remoteErrors != nil && result != "success" {
		instruments.remoteErrors.Add(ctx, 1, attrs)
	}
}

func RecordDBOperation(ctx context.Context, operation string, durationMS int64, err error) {
	attrs := metric.WithAttributes(AttrOperation.String(operation))
	if instruments.dbOperationDuration != nil && durationMS >= 0 {
		instruments.dbOperationDuration.Record(ctx, durationMS, attrs)
	}
	if instruments.dbOperationErrors != nil && err != nil {
		instruments.dbOperationErrors.Add(ctx, 1, attrs)
	}
}

func RecordEventPublished(ctx context.Context, resource, typ string) {
	if instruments.eventsPublished == nil {
		return
	}
	instruments.eventsPublished.Add(ctx, 1, metric.WithAttributes(
		AttrEventResource.String(resource),
		AttrEventType.String(typ),
	))
}

func RecordEventDropped(ctx context.Context) {
	if instruments.eventsDropped == nil {
		return
	}
	instruments.eventsDropped.Add(ctx, 1)
}

func RecordLogsDropped(ctx context.Context, count int64) {
	if instruments.logsDropped == nil || count <= 0 {
		return
	}
	instruments.logsDropped.Add(ctx, count)
}

func RecordEPGProgramsUpserted(ctx context.Context, source, result string, count int64) {
	if instruments.epgProgramsUpserted == nil || source == "" || count <= 0 {
		return
	}
	instruments.epgProgramsUpserted.Add(ctx, count, metric.WithAttributes(
		AttrSource.String(source),
		AttrResult.String(result),
	))
}

func RecordEPGProgramsDeleted(ctx context.Context, source, result string, count int64) {
	if instruments.epgProgramsDeleted == nil || source == "" || count <= 0 {
		return
	}
	instruments.epgProgramsDeleted.Add(ctx, count, metric.WithAttributes(
		AttrSource.String(source),
		AttrResult.String(result),
	))
}

func RecordEPGServiceUpdateError(ctx context.Context, source, result string) {
	if instruments.epgServiceUpdateErrors == nil || source == "" {
		return
	}
	instruments.epgServiceUpdateErrors.Add(ctx, 1, metric.WithAttributes(
		AttrSource.String(source),
		AttrResult.String(result),
	))
}
