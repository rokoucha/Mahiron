package service

import (
	"testing"

	"github.com/21S1298001/Mahiron5/config"
)

func TestServiceManagerGetChannelsExcludesDisabledChannels(t *testing.T) {
	no := false
	yes := true
	manager := NewServiceManager(&ServiceManagerConfig{
		Channels: config.ChannelsConfig{
			{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no},
			{Name: "Disabled", Type: "GR", Channel: "28", IsDisabled: &yes},
		},
	})

	channels := manager.GetChannels()
	if got, want := len(channels), 1; got != want {
		t.Fatalf("channels length = %d, want %d", got, want)
	}
	if got, want := channels[0].Channel, "27"; got != want {
		t.Fatalf("channel = %q, want %q", got, want)
	}
	if channel := manager.GetChannel("GR", "28"); channel != nil {
		t.Fatal("disabled channel should not be returned")
	}
}

func TestServiceManagerUpdateServicesAppendsAndUpdatesByID(t *testing.T) {
	manager := NewServiceManager(&ServiceManagerConfig{})

	manager.updateServices([]*Service{
		{
			Id:          "0000100101",
			ServiceId:   101,
			NetworkId:   1,
			Name:        "NHK",
			ChannelType: "GR",
			ChannelId:   "27",
		},
	})
	manager.updateServices([]*Service{
		{
			Id:          "0000200102",
			ServiceId:   102,
			NetworkId:   2,
			Name:        "BS",
			ChannelType: "BS",
			ChannelId:   "101",
		},
	})

	services := manager.GetServices()
	if got, want := len(services), 2; got != want {
		t.Fatalf("services length = %d, want %d", got, want)
	}

	manager.updateServices([]*Service{
		{
			Id:          "0000100101",
			ServiceId:   101,
			NetworkId:   1,
			Name:        "NHK Updated",
			ChannelType: "GR",
			ChannelId:   "27",
		},
	})

	services = manager.GetServices()
	if got, want := len(services), 2; got != want {
		t.Fatalf("services length after update = %d, want %d", got, want)
	}
	if got, want := manager.GetServiceById("100101").Name, "NHK Updated"; got != want {
		t.Fatalf("updated service name = %q, want %q", got, want)
	}
}
