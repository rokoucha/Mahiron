package epggather

import (
	"time"

	"github.com/21S1298001/mahiron/internal/model"
)

// ReceptionPolicy controls resource budgets, never proof of non-transmission.
type ReceptionPolicy struct {
	RefreshInterval       time.Duration
	ExtendedProbeInterval time.Duration
	UnknownExtendedIdle   time.Duration
}

type CollectOptions struct {
	UnknownExtendedIdle time.Duration
	ProbeExtended       bool
	PreviouslyExtended  map[model.ServiceKey]bool
}

func (o CollectOptions) unknownExtended(update model.ScheduleUpdate, key model.ServiceKey) bool {
	return o.UnknownExtendedIdle > 0 && update.BasicComplete && !update.ExtendedComplete && !update.ExtendedObserved && !update.ExtendedRequired && !o.PreviouslyExtended[key]
}

func (o CollectOptions) canYield(schedule *collectionSchedule, expected []model.ServiceKey) bool {
	if o.ProbeExtended || o.UnknownExtendedIdle <= 0 {
		return false
	}
	partial := false
	for _, key := range expected {
		update := schedule.updates[key]
		if update.BasicComplete && update.ExtendedComplete {
			continue
		}
		if !o.unknownExtended(update, key) {
			return false
		}
		partial = true
	}
	return partial
}
