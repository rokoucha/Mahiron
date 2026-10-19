package epggather

import (
	"fmt"
	"sort"
	"time"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const collectionTraceEventLimit = 64

type collectionTrace struct {
	span                              trace.Span
	started                           time.Time
	events, omitted, versions, resets int
	received                          bool
}

func (t *collectionTrace) event(name string, attrs ...attribute.KeyValue) {
	if !t.span.IsRecording() {
		return
	}
	if t.events >= collectionTraceEventLimit {
		t.omitted++
		return
	}
	t.events++
	attrs = append(attrs, attribute.Int64("epg.elapsed_ms", time.Since(t.started).Milliseconds()))
	t.span.AddEvent(name, trace.WithAttributes(attrs...))
}

func (t *collectionTrace) firstReceive(kind string) {
	if t.received {
		return
	}
	t.received = true
	t.event("epg.first_receive", attribute.String("epg.kind", kind))
}

func serviceTraceAttrs(key model.ServiceKey) []attribute.KeyValue {
	return []attribute.KeyValue{observability.AttrEPGNetworkID.Int(int(key.NetworkID)), attribute.Int("epg.stream_id", int(key.StreamID)), observability.AttrEPGServiceID.Int(int(key.ServiceID))}
}

func (t *collectionTrace) observe(update, previous model.ScheduleUpdate, target bool) {
	t.firstReceive("schedule")
	r := update.Reception
	if r != nil {
		if r.VersionChanged {
			t.versions++
		}
		if r.CoverageReset {
			t.resets++
		}
	}
	basicComplete := target && update.BasicComplete && !previous.BasicComplete
	extendedComplete := target && update.ExtendedObserved && update.ExtendedComplete && (!previous.ExtendedObserved || !previous.ExtendedComplete)
	reset := r != nil && (r.VersionChanged || r.CoverageReset)
	if !t.span.IsRecording() || (!reset && !basicComplete && !extendedComplete) {
		return
	}
	attrs := append(serviceTraceAttrs(update.Service), attribute.Bool("epg.target", target))
	if reset {
		t.event("epg.reception_reset", append(attrs,
			attribute.Int("epg.table_id", int(r.TableID)), attribute.Int("epg.version", int(r.Version)),
			attribute.Int("epg.previous_version", int(r.PreviousVersion)), attribute.Bool("epg.version_changed", r.VersionChanged),
			attribute.Bool("epg.coverage_reset", r.CoverageReset), attribute.Int("epg.section", int(r.Section)),
			attribute.Int("epg.last_section", int(r.LastSection)), attribute.Int("epg.segment_last_section", int(r.SegmentLastSection)),
			attribute.Int("epg.last_table_id", int(r.LastTableID)))...)
	}
	if basicComplete {
		t.event("epg.basic_complete", attrs...)
	}
	if extendedComplete {
		t.event("epg.extended_complete", attrs...)
	}
}

func (t *collectionTrace) finish(schedule *collectionSchedule, expected []model.ServiceKey, reason string, result *CollectResult, collectorErr error) {
	if !t.span.IsRecording() {
		return
	}
	index := newExpectedServiceIndex(expected)
	basic, extended, extendedObserved, otherIncomplete := 0, 0, 0, 0
	var missing, revisions []string
	for _, key := range expected {
		u := schedule.updates[key]
		if u.Revisions != nil && len(revisions) < 32 {
			tableRevisions := u.Revisions()
			sort.Slice(tableRevisions, func(i, j int) bool { return tableRevisions[i].TableID < tableRevisions[j].TableID })
			for _, r := range tableRevisions {
				if len(revisions) >= 32 {
					break
				}
				revisions = append(revisions, fmt.Sprintf("%d/%d/%d table %#02x version %d", key.NetworkID, key.StreamID, key.ServiceID, r.TableID, r.Version))
			}
		}
		if u.BasicComplete {
			basic++
		}
		if u.ExtendedObserved {
			extendedObserved++
			if u.ExtendedComplete {
				extended++
			}
		}
		if !u.BasicComplete || !u.ExtendedComplete {
			if len(missing) < 8 {
				missing = append(missing, fmt.Sprintf("%d/%d/%d: %s", key.NetworkID, key.StreamID, key.ServiceID, schedule.diagnosis(key)))
			}
			t.event("epg.target_incomplete", append(serviceTraceAttrs(key), attribute.Bool("epg.basic_complete", u.BasicComplete), attribute.Bool("epg.extended_complete", u.ExtendedComplete), attribute.String("epg.missing", schedule.diagnosis(key)))...)
		}
	}
	for key, u := range schedule.updates {
		if !index.matchesExpected(key) && (!u.BasicComplete || !u.ExtendedComplete) {
			otherIncomplete++
		}
	}
	t.span.SetAttributes(
		attribute.String("epg.end_reason", reason),
		attribute.StringSlice("epg.missing", missing),
		attribute.StringSlice("epg.target_revisions", revisions),
		attribute.Int("epg.services.basic_complete", basic),
		attribute.Int("epg.services.extended_observed", extendedObserved),
		attribute.Int("epg.services.extended_complete", extended),
		attribute.Int("epg.services.incomplete", len(result.Incomplete)),
		attribute.Int("epg.services.opportunistic_incomplete", otherIncomplete),
		attribute.Int("epg.version_changes", t.versions),
		attribute.Int("epg.coverage_resets", t.resets),
		attribute.Int("epg.events.omitted", t.omitted),
	)
	if collectorErr != nil {
		t.span.SetAttributes(attribute.String("epg.receive_error", collectorErr.Error()))
	}
}
