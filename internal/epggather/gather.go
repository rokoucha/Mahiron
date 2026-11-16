package epggather

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/tuner"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type networkGather struct {
	gatherer                       *Gatherer
	networkID                      uint16
	candidates                     []Candidate
	serviceKeys                    []model.ServiceKey
	remaining                      []model.ServiceKey
	result                         error
	items                          []run.Item
	warnings                       []string
	partial                        bool
	incompleteTotal, deferredTotal int
	onlyUnavailable                bool
	observedTotal, programTotal    int
}

func (g *Gatherer) gatherNetwork(ctx context.Context, networkID uint16, group *Group, option CollectOptions) (err error) {
	n := &networkGather{gatherer: g, networkID: networkID, candidates: group.Candidates, serviceKeys: group.Services,
		remaining: append([]model.ServiceKey(nil), group.Services...), onlyUnavailable: true,
		items: make([]run.Item, 0, len(group.Candidates)+len(group.Services)), warnings: []string{}}
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGGatherNetwork,
		observability.AttrEPGNetworkID.Int(int(networkID)),
		observability.AttrEPGCandidates.Int(len(n.candidates)),
		observability.AttrEPGServices.Int(len(n.serviceKeys)))
	defer func() { observability.EndSpan(span, err) }()
	if len(n.serviceKeys) == 0 {
		return fmt.Errorf("network %d has no known services", networkID)
	}
	ordered, active := g.orderCandidates(n.candidates)
	for _, candidate := range ordered {
		if len(n.remaining) == 0 {
			n.report(ctx, span, true)
			return n.completionError(n.result)
		}
		collected, endReason, candidateErr := n.collectCandidate(ctx, candidate, active[candidate], option)
		n.recordCandidate(candidate, active[candidate], collected, candidateErr, endReason)
		if candidateErr == nil && len(n.remaining) == 0 {
			slog.Debug("finished network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel)
			n.report(ctx, span, true)
			return n.completionError(nil)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if candidateErr != nil {
			slog.Warn("network EPG collection candidate failed", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "remainingServices", len(n.remaining), "err", candidateErr)
			n.warnings = append(n.warnings, fmt.Sprintf("%s/%s: %v", candidate.Type, candidate.Channel, candidateErr))
			n.result = errors.Join(n.result, fmt.Errorf("%s/%s: %w", candidate.Type, candidate.Channel, candidateErr))
		}
	}
	if n.result == nil {
		if len(ordered) == 0 {
			return fmt.Errorf("network %d has no channel candidates", networkID)
		}
		n.result = fmt.Errorf("network %d EITS incomplete for %d services: %w", networkID, len(n.remaining), ErrIncomplete)
	}
	if n.partial {
		n.result = errors.Join(n.result, ErrIncomplete)
	}
	if n.onlyUnavailable && len(ordered) > 0 {
		n.result = errors.Join(n.result, ErrDeferred)
	}
	slog.Warn("network EPG collection failed", "networkId", networkID, "candidates", len(ordered), "err", n.result)
	n.report(ctx, span, false)
	return n.result
}

func (g *Gatherer) orderCandidates(candidates []Candidate) ([]Candidate, map[Candidate]bool) {
	ordered := make([]Candidate, 0, len(candidates))
	active := make(map[Candidate]bool, len(candidates))
	for _, candidate := range candidates {
		if g.streams.HasSession(candidate.Type, candidate.Channel) {
			active[candidate] = true
			ordered = append(ordered, candidate)
		}
	}
	for _, candidate := range candidates {
		if !active[candidate] {
			ordered = append(ordered, candidate)
		}
	}
	return ordered, active
}

func (n *networkGather) collectCandidate(ctx context.Context, candidate Candidate, active bool, option CollectOptions) (*CollectResult, string, error) {
	targets := n.remaining
	slog.Info("starting network EPG collection", "networkId", n.networkID, "type", candidate.Type, "channel", candidate.Channel, "services", len(targets), "activeSession", active)
	candidateCtx, candidateSpan := observability.StartSpan(ctx, observability.SpanEPGGatherCandidate,
		observability.AttrEPGNetworkID.Int(int(n.networkID)),
		observability.AttrChannelType.String(candidate.Type),
		observability.AttrChannelID.String(candidate.Channel),
		observability.AttrStreamActiveSession.Bool(active),
	)
	var candidateErr error
	sessionCtx, cancel := context.WithTimeout(candidateCtx, n.gatherer.retrievalTime)
	source, candidateErr := n.gatherer.streams.OpenSchedule(sessionCtx, candidate.Type, candidate.Channel)
	cancel()
	if candidateErr != nil && ctx.Err() == nil && !errors.Is(candidateErr, tuner.ErrTunerUnavailable) {
		for _, key := range targets {
			if err := n.gatherer.serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, time.Now().UnixMilli(), candidateErr.Error()); err != nil {
				candidateErr = errors.Join(candidateErr, err)
			}
		}
	}
	var collectResult *CollectResult
	if candidateErr == nil {
		if source.IsStored() {
			collectResult, candidateErr = syncStoredPrograms(candidateCtx, n.gatherer.programStore, n.gatherer.serviceStore, source.List, targets, n.gatherer.retrievalTime)
		} else {
			collectResult, candidateErr = CollectServiceSnapshots(candidateCtx, n.gatherer.events, n.gatherer.serviceStore, source.Collect, targets, n.gatherer.retrievalTime, option)
		}
	}
	endReason := ""
	if collectResult != nil {
		endReason = collectResult.EndReason
	}
	if collectResult == nil && candidateErr != nil {
		endReason = "receive_error"
		if errors.Is(candidateErr, context.DeadlineExceeded) {
			endReason = "deadline"
		}
		if ctx.Err() != nil {
			endReason = "canceled"
		}
	}
	if endReason != "" {
		candidateSpan.SetAttributes(attribute.String("epg.end_reason", endReason))
	}
	observability.EndSpan(candidateSpan, candidateErr)
	return collectResult, endReason, candidateErr
}

func (n *networkGather) recordCandidate(candidate Candidate, active bool, collectResult *CollectResult, candidateErr error, endReason string) {
	if !errors.Is(candidateErr, tuner.ErrTunerUnavailable) {
		n.onlyUnavailable = false
	}
	if collectResult != nil {
		n.incompleteTotal += len(collectResult.Incomplete) - len(collectResult.Unobserved)
		n.deferredTotal += len(collectResult.DeferredExtended)
		n.partial = n.partial || len(collectResult.Incomplete) > len(collectResult.Unobserved)
	}
	observedInRemaining := 0
	if collectResult != nil && len(collectResult.Observed) > 0 {
		previousRemaining := len(n.remaining)
		n.programTotal += collectResult.ProgramCount
		n.remaining = serviceKeyDifference(n.remaining, collectResult.Observed)
		observedInRemaining = previousRemaining - len(n.remaining)
		n.observedTotal += observedInRemaining
	}
	item := run.Item{
		Kind:    "candidate",
		Summary: fmt.Sprintf("%s/%s", candidate.Type, candidate.Channel),
		Data: map[string]any{
			"type":          candidate.Type,
			"channel":       candidate.Channel,
			"activeSession": active,
			"observed":      observedInRemaining,
			"remaining":     len(n.remaining),
			"programs":      0,
			"result":        "success",
			"endReason":     endReason,
		},
	}
	if collectResult != nil {
		item.Data["observedServices"] = len(collectResult.Observed)
		item.Data["unobserved"] = len(collectResult.Unobserved)
		item.Data["programs"] = collectResult.ProgramCount
		item.Data["endReason"] = collectResult.EndReason
		incomplete := len(collectResult.Incomplete) - len(collectResult.Unobserved)
		item.Data["incomplete"] = incomplete
		if collectResult.ReceiveError != "" && observedInRemaining > 0 {
			item.Data["result"] = "partial"
			item.Data["receiveError"] = collectResult.ReceiveError
			n.warnings = append(n.warnings, fmt.Sprintf("%s/%s: EPG saved after reception failed: %s", candidate.Type, candidate.Channel, collectResult.ReceiveError))
		}
		if incomplete > 0 {
			item.Data["result"] = "partial"
			n.warnings = append(n.warnings, fmt.Sprintf("%s/%s: %d target services have incomplete EPG (%s)", candidate.Type, candidate.Channel, incomplete, collectResult.EndReason))
		}
	}
	if candidateErr != nil {
		item.Data["result"] = "failure"
		item.Data["error"] = candidateErr.Error()
	}
	n.items = append(n.items, item)
}

// completionError preserves the successful-candidate path's return value while
// retaining earlier errors when incomplete reception needs to be reported.
func (n *networkGather) completionError(success error) error {
	if !n.partial {
		return success
	}
	if n.result == nil && n.incompleteTotal > 0 && n.incompleteTotal == n.deferredTotal {
		return ErrPartial
	}
	return errors.Join(n.result, ErrIncomplete)
}

func (n *networkGather) report(ctx context.Context, span trace.Span, allowPartial bool) {
	report := epgGatherResult(n.networkID, len(n.candidates), len(n.serviceKeys), n.observedTotal, len(n.remaining), n.programTotal, n.items, n.warnings)
	if allowPartial && n.partial && n.incompleteTotal == n.deferredTotal && n.result == nil {
		report.Kind = "epg_partial"
	}
	if len(n.remaining) > 0 {
		for _, key := range n.remaining {
			report.Items = append(report.Items, run.Item{
				Kind:    "service",
				Summary: fmt.Sprintf("service %d", key.ServiceID),
				Data: map[string]any{
					"networkId":         key.NetworkID,
					"transportStreamId": key.StreamID,
					"serviceId":         key.ServiceID,
					"result":            "unobserved",
				},
			})
		}
	}

	run.Set(ctx, report)
	span.SetAttributes(epgGatherAttributes(report)...)
}

func epgGatherResult(networkID uint16, candidateCount, serviceCount, observed, remaining, programs int, items []run.Item, warnings []string) run.Result {
	kind := "epg_gather"
	incomplete := 0
	for _, item := range items {
		if n, ok := item.Data["incomplete"].(int); ok {
			incomplete += n
		}
	}
	summary := fmt.Sprintf("network %d: %d/%d services observed", networkID, observed, serviceCount)
	if remaining > 0 {
		summary = fmt.Sprintf("%s, %d remaining", summary, remaining)
	}
	if incomplete > 0 {
		summary = fmt.Sprintf("%s, %d incomplete", summary, incomplete)
	}
	return run.Result{
		Kind:    kind,
		Summary: summary,
		Counts: map[string]int{
			"candidates":         candidateCount,
			"services":           serviceCount,
			"observedServices":   observed,
			"remainingServices":  remaining,
			"incompleteServices": incomplete,
			"programs":           programs,
		},
		Items:    append([]run.Item(nil), items...),
		Warnings: append([]string(nil), warnings...),
	}
}

func epgGatherAttributes(result run.Result) []attribute.KeyValue {
	return []attribute.KeyValue{
		observability.AttrEPGCandidates.Int(result.Counts["candidates"]),
		observability.AttrEPGServices.Int(result.Counts["services"]),
		observability.AttrEPGServicesObserved.Int(result.Counts["observedServices"]),
		observability.AttrEPGServicesRemaining.Int(result.Counts["remainingServices"]),
		observability.AttrProgramCount.Int(result.Counts["programs"]),
		attribute.Int("epg.services.incomplete", result.Counts["incompleteServices"]),
	}
}

func serviceKeyDifference(keys, remove []model.ServiceKey) []model.ServiceKey {
	seen := make(map[model.ServiceKey]struct{}, len(remove))
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
