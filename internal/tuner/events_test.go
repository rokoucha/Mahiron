package tuner

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
)

type publishedTunerEvent struct {
	typ string
}

type fakeTunerEventPublisher struct {
	events []publishedTunerEvent
}

func (p *fakeTunerEventPublisher) PublishTunerStatusEvent(typ string, _ Status) {
	p.events = append(p.events, publishedTunerEvent{typ: typ})
}

func TestTunerManagerPublishesCreateAndUpdateEvents(t *testing.T) {
	publisher := &fakeTunerEventPublisher{}
	mgr := NewTunerManager(&TunerManagerConfig{
		TunersConfig: config.TunersConfig{{Name: "first", Types: []string{"GR"}, Command: "true"}},
		EventHub:     publisher,
	})

	mgr.SeedEventLog()
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	device.(interface{ AddUser(User) }).AddUser(User{ID: "viewer", Priority: 1})
	if err := device.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	events := publisher.events
	if len(events) < 4 {
		t.Fatalf("events length = %d, want at least 4: %#v", len(events), events)
	}
	if events[0].typ != eventTypeCreate {
		t.Fatalf("first event = %s, want create", events[0].typ)
	}
	for i, event := range events[1:] {
		if event.typ != eventTypeUpdate {
			t.Fatalf("event %d = %s, want update", i+1, event.typ)
		}
	}
}
