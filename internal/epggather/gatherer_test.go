package epggather

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type checkingStreams struct {
	staticEPGStreams
	active    string
	available string
	checked   []string
}

func (s *checkingStreams) HasSession(_ string, channel string) bool { return channel == s.active }
func (s *checkingStreams) CheckAvailable(_ context.Context, _, channel string) error {
	s.checked = append(s.checked, channel)
	if channel == s.available {
		return nil
	}
	return tuner.ErrTunerUnavailable
}

func TestCheckGroupAvailability(t *testing.T) {
	for _, name := range []string{"unavailable", "alternate", "active", "later-active", "stored", "removed"} {
		t.Run(name, func(t *testing.T) {
			streams := &checkingStreams{}
			if name == "active" {
				streams.active = "A"
			}
			if name == "later-active" {
				streams.active = "B"
			}
			if name == "alternate" {
				streams.available = "B"
			}
			key := GroupKey{NetworkID: 4, StreamID: 100, Stored: name == "stored"}
			if name == "removed" {
				key.StreamID = 200
			}
			store := channelServiceStore{remoteSyncServiceStore: newRemoteSyncServiceStore(), services: []*service.Service{
				{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 101}, EITSchedule: true}, ChannelType: "GR", ChannelId: "A"},
				{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 102}, EITSchedule: true}, ChannelType: "GR", ChannelId: "B"},
			}}
			gatherer := NewGatherer(nil, nil, &store, streams, config.ChannelsConfig{{Type: "GR", Channel: "A"}, {Type: "GR", Channel: "B"}}, time.Second)
			err := gatherer.CheckGroup(t.Context(), key)
			if name == "unavailable" {
				if !errors.Is(err, tuner.ErrTunerUnavailable) {
					t.Fatalf("error=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			want := 0
			if name == "unavailable" || name == "alternate" {
				want = 2
			}
			if name == "later-active" {
				want = 1
			}
			if len(streams.checked) != want {
				t.Fatalf("checks=%v want %d", streams.checked, want)
			}
		})
	}
}
