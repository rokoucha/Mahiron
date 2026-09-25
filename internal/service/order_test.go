package service

import (
	"github.com/21S1298001/mahiron/internal/model"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
)

func TestOrderServicesUsesChannelTypeFirstSeenOrder(t *testing.T) {
	yes := true
	manager := NewManager(nil, config.ChannelsConfig{
		{Type: "BS", Channel: "101"},
		{Type: "GR", Channel: "27", IsDisabled: &yes},
		{Type: "GR", Channel: "26"},
		{Type: "BS", Channel: "102"},
		{Type: "CS", Channel: "001"},
	})
	services := []*Service{
		testOrderService("unknown", "SKY", 0, 10, 1, 1),
		testOrderService("cs", "CS", 0, 10, 1, 1),
		testOrderService("gr", "GR", 0, 10, 1, 1),
		testOrderService("bs", "BS", 0, 10, 1, 1),
	}

	got := serviceNames(manager.orderServices(services))
	want := []string{"bs", "gr", "cs", "unknown"}
	if !sameStrings(got, want) {
		t.Fatalf("ordered services = %v, want %v", got, want)
	}
}

func TestOrderServicesSortsRemoteKeysBeforeMissingThenServiceFallbacks(t *testing.T) {
	manager := NewManager(nil, config.ChannelsConfig{{Type: "GR", Channel: "27"}})
	services := []*Service{
		testOrderService("no-key-low-service", "GR", 0, 1, 1, 1),
		testOrderService("key-three", "GR", 3, 99, 1, 1),
		testOrderService("same-service-high-network", "GR", 2, 10, 2, 1),
		testOrderService("same-service-low-tsid", "GR", 2, 10, 1, 1),
		testOrderService("same-service-high-tsid-low-id", "GR", 2, 10, 1, 2),
		testOrderService("key-one", "GR", 1, 99, 1, 1),
		testOrderService("same-service-high-tsid-high-id", "GR", 2, 10, 1, 2),
	}
	services[4].Id = "100"
	services[6].Id = "200"

	got := serviceNames(manager.orderServices(services))
	want := []string{
		"key-one",
		"same-service-low-tsid",
		"same-service-high-tsid-low-id",
		"same-service-high-tsid-high-id",
		"same-service-high-network",
		"key-three",
		"no-key-low-service",
	}
	if !sameStrings(got, want) {
		t.Fatalf("ordered services = %v, want %v", got, want)
	}
}

// testOrderService builds a service; a remoteKey of 0 means no key.
func testOrderService(name, channelType string, remoteKey uint8, serviceID, networkID, transportStreamID uint16) *Service {
	svc := &Service{
		Id: name,
		Service: model.Service{
			Key:  model.ServiceKey{ServiceID: serviceID, NetworkID: networkID, StreamID: transportStreamID},
			Name: name,
		},
		ChannelType: channelType,
	}
	if remoteKey != 0 {
		svc.RemoteControlKey = &remoteKey
	}
	return svc
}

func serviceNames(services []*Service) []string {
	names := make([]string, len(services))
	for i, service := range services {
		names[i] = service.Name
	}
	return names
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
