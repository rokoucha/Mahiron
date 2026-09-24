package mirakurun

import (
	"encoding/json"
	"fmt"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

// RawEventPublisher accepts event payloads that are already encoded, such as
// event.Hub.
type RawEventPublisher interface {
	PublishEventRaw(resource, typ string, raw json.RawMessage)
}

// EventPublisher turns program and service changes into the /api/events
// payloads. The program and service managers publish their own types through
// it, so that neither they nor the event hub know the Mirakurun shape. The
// payload is encoded once here, and the hub keeps the same bytes the API
// serves.
type EventPublisher struct {
	raw RawEventPublisher
}

func NewEventPublisher(raw RawEventPublisher) *EventPublisher {
	return &EventPublisher{raw: raw}
}

func (p *EventPublisher) PublishServiceEvent(typ string, svc *service.Service, channel *config.ChannelConfig) {
	if svc == nil {
		return
	}
	api := ServiceToAPI(svc, channel, true)
	p.raw.PublishEventRaw(event.ResourceService, typ, MarshalService(&api))
}

func (p *EventPublisher) PublishProgramEvent(typ string, program *model.Event) {
	if program == nil {
		return
	}
	api := ProgramToAPI(program)
	p.raw.PublishEventRaw(event.ResourceProgram, typ, MarshalProgram(&api))
}

// PublishProgramRemove publishes a program removal, which carries only the
// program ID.
func (p *EventPublisher) PublishProgramRemove(typ string, id int64) {
	p.raw.PublishEventRaw(event.ResourceProgram, typ, json.RawMessage(fmt.Sprintf(`{"id":%d}`, id)))
}
