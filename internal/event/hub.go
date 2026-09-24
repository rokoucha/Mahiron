package event

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
)

const (
	ResourceProgram     = "program"
	ResourceService     = "service"
	ResourceTuner       = "tuner"
	ResourceJob         = "job"
	ResourceJobSchedule = "job_schedule"

	TypeCreate = "create"
	TypeUpdate = "update"
	TypeRemove = "remove"
)

const defaultLogCapacity = 100

type Event struct {
	Resource string          `json:"resource"`
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
	Time     int64           `json:"time"`
}

type Publisher interface {
	PublishEvent(resource, typ string, data any)
}

type Hub struct {
	mu          sync.Mutex
	capacity    int
	log         []Event
	subscribers map[chan Event]struct{}
	now         func() time.Time
}

func New() *Hub {
	return NewWithCapacity(defaultLogCapacity)
}

func NewWithCapacity(capacity int) *Hub {
	if capacity <= 0 {
		capacity = defaultLogCapacity
	}
	return &Hub{
		capacity:    capacity,
		subscribers: map[chan Event]struct{}{},
		now:         time.Now,
	}
}

func (h *Hub) PublishEvent(resource, typ string, data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		return
	}
	observability.RecordEventPublished(context.Background(), resource, typ)
	event := Event{
		Resource: resource,
		Type:     typ,
		Data:     append(json.RawMessage(nil), raw...),
		Time:     h.now().UnixMilli(),
	}

	h.mu.Lock()
	h.log = append(h.log, event)
	if overflow := len(h.log) - h.capacity; overflow > 0 {
		h.log = append([]Event(nil), h.log[overflow:]...)
	}
	for ch := range h.subscribers {
		select {
		case ch <- cloneEvent(event):
		default:
			observability.RecordEventDropped(context.Background())
		}
	}
	h.mu.Unlock()
}

func (h *Hub) PublishEventRaw(resource, typ string, raw json.RawMessage) {
	raw = append(json.RawMessage(nil), raw...)
	observability.RecordEventPublished(context.Background(), resource, typ)
	event := Event{
		Resource: resource,
		Type:     typ,
		Data:     raw,
		Time:     h.now().UnixMilli(),
	}

	h.mu.Lock()
	h.log = append(h.log, event)
	if overflow := len(h.log) - h.capacity; overflow > 0 {
		h.log = append([]Event(nil), h.log[overflow:]...)
	}
	for ch := range h.subscribers {
		select {
		case ch <- cloneEvent(event):
		default:
			observability.RecordEventDropped(context.Background())
		}
	}
	h.mu.Unlock()
}

// PublishServiceEvent stores the Mirakurun-compatible service payload. The
// payload is encoded here so the log holds the same bytes the API serves.
func (h *Hub) PublishServiceEvent(typ string, svc *service.Service, channel *config.ChannelConfig) {
	if svc == nil {
		return
	}
	api := mirakurun.ServiceToAPI(svc, channel, true)
	h.PublishEventRaw(ResourceService, typ, mirakurun.MarshalService(&api))
}

// PublishProgramEvent stores the Mirakurun-compatible program payload. The
// payload is encoded here so the log holds the same bytes the API serves.
func (h *Hub) PublishProgramEvent(typ string, p *program.Program) {
	if p == nil {
		return
	}
	api := mirakurun.ProgramToAPI(p)
	h.PublishEventRaw(ResourceProgram, typ, mirakurun.MarshalProgram(&api))
}

// PublishProgramRemove stores a program removal carrying only the program ID.
func (h *Hub) PublishProgramRemove(typ string, id int64) {
	h.PublishEventRaw(ResourceProgram, typ, json.RawMessage(fmt.Sprintf(`{"id":%d}`, id)))
}

func (h *Hub) PublishTunerStatusEvent(typ string, data map[string]any) {
	h.PublishEvent(ResourceTuner, typ, data)
}

func (h *Hub) PublishJobEvent(typ string, data map[string]any) {
	h.PublishEvent(ResourceJob, typ, data)
}

func (h *Hub) PublishJobScheduleEvent(typ string, data map[string]any) {
	h.PublishEvent(ResourceJobSchedule, typ, data)
}

func (h *Hub) Log() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	events := make([]Event, len(h.log))
	for i := range h.log {
		events[i] = cloneEvent(h.log[i])
	}
	return events
}

func (h *Hub) SubscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subscribers)
}

func (h *Hub) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 128)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subscribers, ch)
			close(ch)
			h.mu.Unlock()
		})
	}
	return ch, unsubscribe
}

func cloneEvent(event Event) Event {
	event.Data = append(json.RawMessage(nil), event.Data...)
	return event
}
