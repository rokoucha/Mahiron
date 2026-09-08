package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	SpanAPIGetPrograms                    = "api.get_programs"
	SpanAPIIptvXmltv                      = "api.iptv_xmltv"
	SpanDBProgramDeleteEndedBefore        = "db.program.delete_ended_before"
	SpanDBProgramReplaceServicePrograms   = "db.program.replace_service_programs"
	SpanDBProgramUpsertAll                = "db.program.upsert_all"
	SpanDBServiceEPGSummary               = "db.service.epg_summary"
	SpanDBServiceReplaceChannelServices   = "db.service.replace_channel_services"
	SpanEPGCollectServiceSnapshots        = "epg.collect_service_snapshots"
	SpanEPGGatherCandidate                = "epg.gather_candidate"
	SpanEPGGatherNetwork                  = "epg.gather_network"
	SpanEPGMergeServicePrograms           = "epg.merge_service_programs"
	SpanEPGReplaceRemoteServicePrograms   = "epg.replace_remote_service_programs"
	SpanEPGSyncStoredServicePrograms      = "epg.sync_stored_service_programs"
	SpanJobRun                            = "job.run"
	SpanRemoteListServicePrograms         = "remote.list_service_programs"
	SpanRemoteScanServices                = "remote.scan_services"
	SpanRemoteStreamProgramEventsConnect  = "remote.stream_program_events.connect"
	SpanServiceScanReplaceChannelServices = "service_scan.replace_channel_services"
	SpanServiceScanRunScanner             = "service_scan.run_scanner"
	SpanServiceScanScanChannel            = "service_scan.scan_channel"
	SpanStreamGetOrCreate                 = "stream.get_or_create"
	SpanStreamSourceAcquire               = "stream.source.acquire"
	SpanStreamSourceSelectRoute           = "stream.source.select_route"
	SpanStreamSourceTryRoute              = "stream.source.try_route"
	SpanTunerAcquireDevice                = "tuner.acquire_device"
	SpanTunerProcessStart                 = "tuner.process.start"
	SpanTunerProcessStartWithRetry        = "tuner.process.start_with_retry"
)

func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(instrumentationName).Start(ctx, name, trace.WithAttributes(attrs...))
}

func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
