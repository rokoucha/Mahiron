package api

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/service"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func testListHandler() *Handler {
	no := false
	yes := true
	return NewHandler(HandlerConfig{
		ProgramManager: program.NewProgramManager(nil),
		ServiceManager: service.NewServiceManager(&service.ServiceManagerConfig{
			Channels: config.ChannelsConfig{
				{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no},
				{Name: "BS", Type: "BS", Channel: "101", IsDisabled: &no},
				{Name: "Disabled", Type: "GR", Channel: "28", IsDisabled: &yes},
			},
			Services: []*service.Service{
				{
					Id:                 "0000100101",
					ServiceId:          101,
					NetworkId:          1,
					TransportStreamId:  10,
					Name:               "NHK Service",
					Type:               1,
					RemoteControlKeyId: 3,
					ChannelType:        "GR",
					ChannelId:          "27",
				},
				{
					Id:                 "0000200102",
					ServiceId:          102,
					NetworkId:          2,
					TransportStreamId:  20,
					Name:               "BS Service",
					Type:               1,
					RemoteControlKeyId: 4,
					ChannelType:        "BS",
					ChannelId:          "101",
				},
			},
		}),
	})
}

func TestGetChannelsReturnsEnabledChannelsWithServices(t *testing.T) {
	handler := testListHandler()

	res, err := handler.GetChannels(context.Background(), apigen.GetChannelsParams{})
	if err != nil {
		t.Fatal(err)
	}
	channels, ok := res.(*apigen.GetChannelsOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetChannelsOKApplicationJSON", res)
	}
	if got, want := len(*channels), 2; got != want {
		t.Fatalf("channels length = %d, want %d", got, want)
	}
	if got, want := (*channels)[0].Channel, "27"; got != want {
		t.Fatalf("first channel = %q, want %q", got, want)
	}
	if got, want := len((*channels)[0].Services), 1; got != want {
		t.Fatalf("first channel services length = %d, want %d", got, want)
	}
	if got, want := len((*channels)[0].Routes), 1; got != want {
		t.Fatalf("first channel routes length = %d, want %d", got, want)
	}
	if got, want := (*channels)[0].Routes[0].Type, "GR"; got != want {
		t.Fatalf("first channel route type = %q, want %q", got, want)
	}
}

func TestGetChannelReturnsNotFoundForDisabledChannel(t *testing.T) {
	handler := testListHandler()

	res, err := handler.GetChannel(context.Background(), apigen.GetChannelParams{
		Type:    "GR",
		Channel: "28",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ErrorStatusCode); !ok {
		t.Fatalf("response type = %T, want *ErrorStatusCode", res)
	}
}

func TestGetServicesReturnsServicesWithChannelsAndFilters(t *testing.T) {
	handler := testListHandler()

	res, err := handler.GetServices(context.Background(), apigen.GetServicesParams{
		ChannelType: apigen.NewOptString("BS"),
	})
	if err != nil {
		t.Fatal(err)
	}
	services, ok := res.(*apigen.GetServicesOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetServicesOKApplicationJSON", res)
	}
	if got, want := len(*services), 1; got != want {
		t.Fatalf("services length = %d, want %d", got, want)
	}
	if got, want := (*services)[0].Name, "BS Service"; got != want {
		t.Fatalf("service name = %q, want %q", got, want)
	}
	channel, ok := (*services)[0].Channel.Get()
	if !ok {
		t.Fatal("service channel should be set")
	}
	if got, want := channel.Channel, "101"; got != want {
		t.Fatalf("service channel = %q, want %q", got, want)
	}
}

func TestGetServicesByChannelAndGetServiceByChannel(t *testing.T) {
	handler := testListHandler()

	listRes, err := handler.GetServicesByChannel(context.Background(), apigen.GetServicesByChannelParams{
		Type:    "GR",
		Channel: "27",
	})
	if err != nil {
		t.Fatal(err)
	}
	services, ok := listRes.(*apigen.GetServicesByChannelOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetServicesByChannelOKApplicationJSON", listRes)
	}
	if got, want := len(*services), 1; got != want {
		t.Fatalf("services length = %d, want %d", got, want)
	}

	serviceRes, err := handler.GetServiceByChannel(context.Background(), apigen.GetServiceByChannelParams{
		Type:    "GR",
		Channel: "27",
		ID:      100101,
	})
	if err != nil {
		t.Fatal(err)
	}
	serviceList, ok := serviceRes.(*apigen.GetServiceByChannelOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetServiceByChannelOKApplicationJSON", serviceRes)
	}
	if got, want := len(*serviceList), 1; got != want {
		t.Fatalf("service list length = %d, want %d", got, want)
	}
}

func TestGetServiceReturnsNotFound(t *testing.T) {
	handler := testListHandler()

	res, err := handler.GetService(context.Background(), apigen.GetServiceParams{ID: 999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ErrorStatusCode); !ok {
		t.Fatalf("response type = %T, want *ErrorStatusCode", res)
	}
}
