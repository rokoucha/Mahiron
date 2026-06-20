package event

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/tuner"
)

const (
	ResourceProgram = "program"
	ResourceService = "service"
	ResourceTuner   = "tuner"

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
		}
	}
	h.mu.Unlock()
}

func (h *Hub) PublishServiceEvent(typ string, svc *service.Service, channel *config.ChannelConfig) {
	if svc == nil {
		return
	}
	h.PublishEvent(ResourceService, typ, serviceEventData(svc, channel))
}

func (h *Hub) PublishTunerStatusEvent(typ string, status tuner.Status) {
	h.PublishEvent(ResourceTuner, typ, tunerEventData(status))
}

func (h *Hub) PublishProgramEvent(typ string, p *program.Program) {
	if p == nil {
		return
	}
	h.PublishEvent(ResourceProgram, typ, programEventData(p))
}

func (h *Hub) PublishProgramRemoveEvent(id int64) {
	h.PublishEvent(ResourceProgram, TypeRemove, programRemoveEventData(id))
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
