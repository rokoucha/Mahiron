package epggather

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/service"
	"go.opentelemetry.io/otel/attribute"
)

// persistObservedSnapshots keeps per-service writes in reception order. A single
// timestamp is shared by every attempt, reception and success update.
func (c *collectionRun) persistObservedSnapshots(ctx context.Context, receiveErr error) error {
	updatedAt := c.schedule.nowMillis()
	mergeKeys, expectedSeen, observedEvents := c.prepareSnapshots()
	var collectErr, unobserved error
	expectedObserved := 0
	for _, key := range mergeKeys {
		_, isExpected := expectedSeen[key]
		if c.schedule.observed(key) {
			if isExpected {
				expectedObserved++
			}
			collectErr = errors.Join(collectErr, c.persistObservedService(ctx, key, observedEvents[key], isExpected, updatedAt, receiveErr))
		} else if isExpected {
			if c.schedule.updates[key].ExtendedObserved {
				if err := c.serviceStore.SetEPGReception(ctx, key.NetworkID, key.ServiceID, service.EPGReception{UpdatedAt: updatedAt, ExtendedObserved: true, PreserveOutcome: true}); err != nil {
					collectErr = errors.Join(collectErr, err)
				}
			}
			c.result.Unobserved = append(c.result.Unobserved, key)
			c.result.Incomplete = append(c.result.Incomplete, key)
			slog.Warn("target EPG not received",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"report", c.schedule.diagnosis(key))
			err := fmt.Errorf("service %d EITS incomplete", key.ServiceID)
			if receiveErr != nil {
				err = fmt.Errorf("service %d EPG reception failed: %w", key.ServiceID, receiveErr)
			}
			if attemptErr := c.serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
			}
			unobserved = errors.Join(unobserved, err)
		}
	}
	if expectedObserved == 0 {
		collectErr = errors.Join(collectErr, unobserved)
	}
	return collectErr
}

func (c *collectionRun) prepareSnapshots() ([]model.ServiceKey, map[model.ServiceKey]struct{}, map[model.ServiceKey][]model.Event) {
	observedEvents := make(map[model.ServiceKey][]model.Event)
	mergeKeys := append([]model.ServiceKey(nil), c.expected...)
	expectedSeen := make(map[model.ServiceKey]struct{}, len(c.expected))
	expectedTransportStreams := make(map[serviceIdentity]uint16, len(c.expected))
	for _, key := range c.expected {
		expectedSeen[key] = struct{}{}
		expectedTransportStreams[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}] = key.StreamID
	}
	for key := range c.observedServices {
		if _, ok := expectedSeen[key]; ok || !c.schedule.observed(key) {
			continue
		}
		if expectedTSID, ok := expectedTransportStreams[serviceIdentity{networkID: key.NetworkID, serviceID: key.ServiceID}]; ok && expectedTSID != 0 && key.StreamID != expectedTSID {
			continue
		}
		mergeKeys = append(mergeKeys, key)
	}
	var shared [][]model.Event
	for _, key := range mergeKeys {
		if !c.schedule.observed(key) {
			continue
		}
		observedEvents[key] = c.schedule.events(key)
		shared = append(shared, observedEvents[key])
	}
	fillEventsFromSharedPeers(shared...)
	return mergeKeys, expectedSeen, observedEvents
}

func (c *collectionRun) persistObservedService(ctx context.Context, key model.ServiceKey, events []model.Event, isExpected bool, updatedAt int64, receiveErr error) error {
	inTargetStream := c.index.matchesCollectionStream(key)

	c.result.ProgramCount += len(events)
	update := c.schedule.updates[key]
	incomplete := !update.BasicComplete || !update.ExtendedComplete
	if isExpected && incomplete {
		diagnosis := c.schedule.diagnosis(key)
		c.result.Incomplete = append(c.result.Incomplete, key)
		slog.Warn("target EPG collection incomplete", "networkId", key.NetworkID, "serviceId", key.ServiceID, "basicComplete", update.BasicComplete, "extendedComplete", update.ExtendedComplete, "report", diagnosis)
	}
	mergeCtx, mergeSpan := observability.StartSpan(ctx, observability.SpanEPGMergeServicePrograms,
		observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
		observability.AttrEPGServiceID.Int(int(key.ServiceID)),
		observability.AttrProgramCount.Int(len(events)),
		attribute.Bool("epg.target", isExpected), attribute.Bool("epg.basic_complete", update.BasicComplete), attribute.Bool("epg.extended_complete", update.ExtendedComplete),
	)
	mergeCtx = observability.ContextWithEPGMetricSource(mergeCtx, "eits")
	err := c.events.UpsertSchedule(mergeCtx, c.schedule.snapshot(key, events))
	observability.EndSpan(mergeSpan, err)
	if err != nil {
		if inTargetStream {
			if attemptErr := c.serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
			}
		}
		return fmt.Errorf("service %d: merge: %w", key.ServiceID, err)
	}
	if inTargetStream && (isExpected || !incomplete) {
		c.result.Observed = append(c.result.Observed, key)
	}
	return c.recordReception(ctx, key, events, isExpected, updatedAt, receiveErr)
}

func (c *collectionRun) recordReception(ctx context.Context, key model.ServiceKey, events []model.Event, isExpected bool, updatedAt int64, receiveErr error) error {
	var collectErr error
	update := c.schedule.updates[key]
	incomplete := !update.BasicComplete || !update.ExtendedComplete
	// Opportunistic partial data must not make a future target look fresh.
	if !c.index.matchesCollectionStream(key) {
		return collectErr
	}
	if !isExpected && incomplete {
		if update.ExtendedObserved {
			if err := c.serviceStore.SetEPGReception(ctx, key.NetworkID, key.ServiceID, service.EPGReception{UpdatedAt: updatedAt, ExtendedObserved: true, PreserveOutcome: true}); err != nil {
				collectErr = errors.Join(collectErr, err)
			}
		}
		return collectErr
	}
	plannedPartial := isExpected && receiveErr == nil && (c.result.EndReason == "unknown_extended_idle" || c.result.EndReason == "deadline") && c.option.unknownExtended(update, key)
	reception := service.EPGReception{UpdatedAt: updatedAt, BasicComplete: update.BasicComplete, ExtendedObserved: update.ExtendedObserved, ExtendedProbed: !incomplete || c.option.ProbeExtended && c.result.EndReason == "deadline" && receiveErr == nil && update.BasicComplete, Partial: plannedPartial}
	if err := c.serviceStore.SetEPGReception(ctx, key.NetworkID, key.ServiceID, reception); err != nil {
		collectErr = errors.Join(collectErr, err)
		plannedPartial = false
	}
	if plannedPartial {
		c.result.DeferredExtended = append(c.result.DeferredExtended, key)
	}
	if incomplete {
		warning := "EPG reception incomplete: " + c.schedule.diagnosis(key)
		if quality := lowQualityEventWarning(events); quality != "" {
			warning += "; " + quality
		}
		if receiveErr != nil {
			warning += ": " + receiveErr.Error()
		}
		if err := c.serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, warning); err != nil {
			collectErr = errors.Join(collectErr, err)
		}
		return collectErr
	}
	if err := c.serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, updatedAt); err != nil {
		observability.RecordEPGServiceUpdateError(ctx, "eits", "success")
		collectErr = errors.Join(collectErr, err)
	}
	if warning := lowQualityEventWarning(events); warning != "" {
		slog.Warn("EITS collection quality is low", "networkId", key.NetworkID, "serviceId", key.ServiceID, "warning", warning)
		if attemptErr := c.serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, warning); attemptErr != nil {
			observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
		}
	}
	return collectErr
}
