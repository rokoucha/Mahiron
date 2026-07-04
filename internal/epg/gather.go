package epg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/jobreport"
	"github.com/21S1298001/mahiron/internal/observability"
	"go.opentelemetry.io/otel/attribute"
)

func gatherNetwork(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, streams StreamManager, networkID uint16, candidates []Candidate, serviceKeys []ServiceKey, retrievalTime time.Duration) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGGatherNetwork,
		observability.AttrEPGNetworkID.Int(int(networkID)),
		observability.AttrEPGCandidates.Int(len(candidates)),
		observability.AttrEPGServices.Int(len(serviceKeys)),
	)
	defer func() { observability.EndSpan(span, err) }()

	if len(serviceKeys) == 0 {
		return fmt.Errorf("network %d has no known services", networkID)
	}
	ordered := make([]Candidate, 0, len(candidates))
	active := make(map[Candidate]bool, len(candidates))
	for _, candidate := range candidates {
		if streams.HasSession(candidate.Type, candidate.Channel) {
			active[candidate] = true
			ordered = append(ordered, candidate)
		}
	}
	for _, candidate := range candidates {
		if !active[candidate] {
			ordered = append(ordered, candidate)
		}
	}
	remaining := append([]ServiceKey(nil), serviceKeys...)
	var result error
	items := make([]jobreport.Item, 0, len(ordered)+len(serviceKeys))
	warnings := []string{}
	observedTotal := 0
	programTotal := 0
	for _, candidate := range ordered {
		if len(remaining) == 0 {
			report := epgGatherResult(networkID, len(candidates), len(serviceKeys), observedTotal, len(remaining), programTotal, items, warnings)
			jobreport.Set(ctx, report)
			span.SetAttributes(epgGatherAttributes(report)...)
			return nil
		}
		slog.Info("starting network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "services", len(remaining), "activeSession", active[candidate])
		candidateCtx, candidateSpan := observability.StartSpan(ctx, observability.SpanEPGGatherCandidate,
			observability.AttrEPGNetworkID.Int(int(networkID)),
			observability.AttrChannelType.String(candidate.Type),
			observability.AttrChannelID.String(candidate.Channel),
			observability.AttrStreamActiveSession.Bool(active[candidate]),
		)
		var candidateErr error
		sessionCtx, cancel := context.WithTimeout(candidateCtx, retrievalTime)
		session, candidateErr := streams.GetOrCreateWait(sessionCtx, candidate.Type, candidate.Channel)
		cancel()
		var collectResult *CollectResult
		if candidateErr == nil {
			collectResult, candidateErr = CollectServiceSnapshots(candidateCtx, programStore, serviceStore, session, remaining, retrievalTime)
		}
		observability.EndSpan(candidateSpan, candidateErr)
		observedInRemaining := 0
		if collectResult != nil && len(collectResult.Observed) > 0 {
			previousRemaining := len(remaining)
			programTotal += collectResult.ProgramCount
			remaining = serviceKeyDifference(remaining, collectResult.Observed)
			observedInRemaining = previousRemaining - len(remaining)
			observedTotal += observedInRemaining
		}
		item := jobreport.Item{
			Kind:    "candidate",
			Summary: fmt.Sprintf("%s/%s", candidate.Type, candidate.Channel),
			Data: map[string]any{
				"type":          candidate.Type,
				"channel":       candidate.Channel,
				"activeSession": active[candidate],
				"observed":      observedInRemaining,
				"remaining":     len(remaining),
				"programs":      0,
				"result":        "success",
			},
		}
		if collectResult != nil {
			item.Data["observedServices"] = len(collectResult.Observed)
			item.Data["unobserved"] = len(collectResult.Unobserved)
			item.Data["programs"] = collectResult.ProgramCount
		}
		if candidateErr != nil {
			item.Data["result"] = "failure"
			item.Data["error"] = candidateErr.Error()
		}
		items = append(items, item)
		if candidateErr == nil && len(remaining) == 0 {
			slog.Debug("finished network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel)
			report := epgGatherResult(networkID, len(candidates), len(serviceKeys), observedTotal, len(remaining), programTotal, items, warnings)
			jobreport.Set(ctx, report)
			span.SetAttributes(epgGatherAttributes(report)...)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if candidateErr != nil {
			slog.Warn("network EPG collection candidate failed", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "remainingServices", len(remaining), "err", candidateErr)
			warnings = append(warnings, fmt.Sprintf("%s/%s: %v", candidate.Type, candidate.Channel, candidateErr))
			result = errors.Join(result, fmt.Errorf("%s/%s: %w", candidate.Type, candidate.Channel, candidateErr))
		}
	}
	if result == nil {
		if len(ordered) == 0 {
			return fmt.Errorf("network %d has no channel candidates", networkID)
		}
		result = fmt.Errorf("network %d EITS incomplete for %d services", networkID, len(remaining))
	}
	slog.Warn("network EPG collection failed", "networkId", networkID, "candidates", len(ordered), "err", result)
	report := epgGatherResult(networkID, len(candidates), len(serviceKeys), observedTotal, len(remaining), programTotal, items, warnings)
	if len(remaining) > 0 {
		for _, key := range remaining {
			report.Items = append(report.Items, jobreport.Item{
				Kind:    "service",
				Summary: fmt.Sprintf("service %d", key.ServiceID),
				Data: map[string]any{
					"networkId":         key.NetworkID,
					"transportStreamId": key.TransportStreamID,
					"serviceId":         key.ServiceID,
					"result":            "unobserved",
				},
			})
		}
	}
	jobreport.Set(ctx, report)
	span.SetAttributes(epgGatherAttributes(report)...)
	return result
}

func epgGatherResult(networkID uint16, candidateCount, serviceCount, observed, remaining, programs int, items []jobreport.Item, warnings []string) jobreport.Result {
	kind := "epg_gather"
	summary := fmt.Sprintf("network %d: %d/%d services observed", networkID, observed, serviceCount)
	if remaining > 0 {
		summary = fmt.Sprintf("%s, %d remaining", summary, remaining)
	}
	return jobreport.Result{
		Kind:    kind,
		Summary: summary,
		Counts: map[string]int{
			"candidates":        candidateCount,
			"services":          serviceCount,
			"observedServices":  observed,
			"remainingServices": remaining,
			"programs":          programs,
		},
		Items:    append([]jobreport.Item(nil), items...),
		Warnings: append([]string(nil), warnings...),
	}
}

func epgGatherAttributes(result jobreport.Result) []attribute.KeyValue {
	return []attribute.KeyValue{
		observability.AttrEPGCandidates.Int(result.Counts["candidates"]),
		observability.AttrEPGServices.Int(result.Counts["services"]),
		observability.AttrEPGServicesObserved.Int(result.Counts["observedServices"]),
		observability.AttrEPGServicesRemaining.Int(result.Counts["remainingServices"]),
		observability.AttrProgramCount.Int(result.Counts["programs"]),
	}
}

func serviceKeyDifference(keys, remove []ServiceKey) []ServiceKey {
	seen := make(map[ServiceKey]struct{}, len(remove))
	for _, key := range remove {
		seen[key] = struct{}{}
	}
	out := keys[:0]
	for _, key := range keys {
		if _, ok := seen[key]; !ok {
			out = append(out, key)
		}
	}
	return out
}
