package program

import "github.com/21S1298001/mahiron/internal/model"

// Program is a stored program: the broadcast event and the ID Mahiron
// assigns it.
type Program struct {
	ID int64
	model.Event
}

// FromEvent builds the program for an event.
func FromEvent(event model.Event) *Program {
	return &Program{ID: model.ProgramID(event.Key, event.EventID), Event: event}
}

type Query struct {
	ID        *int64
	NetworkID *uint16
	ServiceID *uint16
	EventID   *uint16
	StartAt   *int64
	EndAt     *int64
}

func ProgramID(networkID, serviceID, eventID uint16) int64 {
	return model.ProgramID(model.ServiceKey{NetworkID: networkID, ServiceID: serviceID}, eventID)
}

// StartAtOrZero returns the start time in Unix milliseconds, or 0 when it is
// undecided.
func (p *Program) StartAtOrZero() int64 {
	if p.StartAt == nil {
		return 0
	}
	return *p.StartAt
}

// DurationOrZero returns the duration in milliseconds, or 0 when it is
// undecided.
func (p *Program) DurationOrZero() int {
	if p.DurationMS == nil {
		return 0
	}
	return *p.DurationMS
}
