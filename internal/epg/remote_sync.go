package epg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
)

// minStartAt returns the smallest StartAt among programs, or 0 if programs
// is empty. It is used as the lower bound for ReplaceServicePrograms so a
// sync only rewrites the window of programs actually received from the
// remote, instead of unconditionally replacing everything from time zero.
func minStartAt(programs []*program.Program) int64 {
	var min int64
	first := true
	for _, p := range programs {
		if p == nil {
			continue
		}
		if first || p.StartAt < min {
			min = p.StartAt
			first = false
		}
	}
	return min
}

func syncStoredServicePrograms(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, lister StoredProgramLister, expected []ServiceKey, retrievalTime time.Duration) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGSyncStoredServicePrograms,
		observability.AttrEPGServices.Int(len(expected)),
		observability.AttrEPGRetrievalTimeMS.Int64(retrievalTime.Milliseconds()),
	)
	defer func() { observability.EndSpan(span, err) }()

	syncCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	var result error
	for _, key := range expected {
		if err := syncCtx.Err(); err != nil {
			return errors.Join(result, err)
		}
		programs, err := lister.ListServicePrograms(syncCtx, key.NetworkID, key.ServiceID)
		now := time.Now().UnixMilli()
		if err != nil {
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "remote", "attempt")
			}
			result = errors.Join(result, fmt.Errorf("service %d: list remote programs: %w", key.ServiceID, err))
			continue
		}
		slog.Info("syncing stored remote EPG", "networkId", key.NetworkID, "serviceId", key.ServiceID, "programs", len(programs))
		replaceCtx, replaceSpan := observability.StartSpan(ctx, observability.SpanEPGReplaceRemoteServicePrograms,
			observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
			observability.AttrEPGServiceID.Int(int(key.ServiceID)),
			observability.AttrProgramCount.Int(len(programs)),
		)
		replaceCtx = observability.ContextWithEPGMetricSource(replaceCtx, "remote")
		err = programStore.ReplaceServicePrograms(replaceCtx, key.NetworkID, key.ServiceID, minStartAt(programs), programs)
		observability.EndSpan(replaceSpan, err)
		if err != nil {
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "remote", "attempt")
			}
			result = errors.Join(result, fmt.Errorf("service %d: replace remote programs: %w", key.ServiceID, err))
			continue
		}
		if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, now); err != nil {
			observability.RecordEPGServiceUpdateError(ctx, "remote", "success")
			result = errors.Join(result, err)
		}
	}
	return result
}
