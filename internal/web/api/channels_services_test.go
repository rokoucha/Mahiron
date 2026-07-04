package api

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func testListHandler(t *testing.T) *Handler {
	t.Helper()
	ctx := context.Background()
	no := false
	yes := true
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	serviceStore := service.NewSQLiteStore(database)
	services := []*service.Service{
		{
			Id:                  "0000100101",
			ServiceId:           101,
			NetworkId:           1,
			TransportStreamId:   10,
			Name:                "NHK Service",
			Type:                1,
			EITScheduleFlag:     true,
			EITPresentFollowing: true,
			RemoteControlKeyId:  3,
			ChannelType:         "GR",
			ChannelId:           "27",
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
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{services[0]}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "101", []*service.Service{services[1]}); err != nil {
		t.Fatal(err)
	}
	return NewHandler(HandlerConfig{
		ProgramManager: program.NewProgramManager(program.NewSQLiteStore(database)),
		ServiceManager: service.NewServiceManager(serviceStore, config.ChannelsConfig{
			{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no},
			{Name: "BS", Type: "BS", Channel: "101", IsDisabled: &no},
			{Name: "Disabled", Type: "GR", Channel: "28", IsDisabled: &yes},
		}),
	})
}

func TestGetChannelsReturnsEnabledChannelsWithServices(t *testing.T) {
	handler := testListHandler(t)

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
	if got, ok := (*channels)[0].Services[0].EitScheduleFlag.Get(); !ok || !got {
		t.Fatalf("first channel service EitScheduleFlag = %v, %v, want true, true", got, ok)
	}
	if got, ok := (*channels)[0].Services[0].EitPresentFollowing.Get(); !ok || !got {
		t.Fatalf("first channel service EitPresentFollowing = %v, %v, want true, true", got, ok)
	}
}

func TestGetChannelsPropagatesStoreError(t *testing.T) {
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	store := service.NewSQLiteStore(database)
	handler := NewHandler(HandlerConfig{
		ServiceManager: service.NewServiceManager(store, config.ChannelsConfig{{Type: "GR", Channel: "27"}}),
	})
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.GetChannels(context.Background(), apigen.GetChannelsParams{}); err == nil {
		t.Fatal("GetChannels succeeded after database was closed")
	}
}

func TestGetChannelReturnsNotFoundForDisabledChannel(t *testing.T) {
	handler := testListHandler(t)

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
	handler := testListHandler(t)

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

func TestServiceListEndpointsReturnServerOrder(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store := service.NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{
			Id:                 "0000100103",
			ServiceId:          103,
			NetworkId:          1,
			TransportStreamId:  1,
			Name:               "GR 3",
			Type:               1,
			RemoteControlKeyId: 3,
		},
		{
			Id:                "0000100101",
			ServiceId:         101,
			NetworkId:         1,
			TransportStreamId: 1,
			Name:              "GR no key",
			Type:              1,
		},
		{
			Id:                 "0000100102",
			ServiceId:          102,
			NetworkId:          1,
			TransportStreamId:  1,
			Name:               "GR 1",
			Type:               1,
			RemoteControlKeyId: 1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "101", []*service.Service{
		{
			Id:                "0000200201",
			ServiceId:         201,
			NetworkId:         2,
			TransportStreamId: 1,
			Name:              "BS",
			Type:              1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(HandlerConfig{
		ProgramManager: program.NewProgramManager(program.NewSQLiteStore(database)),
		ServiceManager: service.NewServiceManager(store, config.ChannelsConfig{
			{Name: "BS", Type: "BS", Channel: "101"},
			{Name: "GR", Type: "GR", Channel: "27"},
		}),
	})

	res, err := handler.GetServices(ctx, apigen.GetServicesParams{})
	if err != nil {
		t.Fatal(err)
	}
	services, ok := res.(*apigen.GetServicesOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetServicesOKApplicationJSON", res)
	}
	if got, want := apiServiceNames(*services), []string{"BS", "GR 1", "GR 3", "GR no key"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("services = %v, want %v", got, want)
	}

	channelRes, err := handler.GetServicesByChannel(ctx, apigen.GetServicesByChannelParams{
		Type:    "GR",
		Channel: "27",
	})
	if err != nil {
		t.Fatal(err)
	}
	channelServices, ok := channelRes.(*apigen.GetServicesByChannelOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetServicesByChannelOKApplicationJSON", channelRes)
	}
	if got, want := apiServiceNames(*channelServices), []string{"GR 1", "GR 3", "GR no key"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("channel services = %v, want %v", got, want)
	}
}

func TestGetServicesByChannelAndGetServiceByChannel(t *testing.T) {
	handler := testListHandler(t)

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
	serviceItem, ok := serviceRes.(*apigen.Service)
	if !ok {
		t.Fatalf("response type = %T, want *Service", serviceRes)
	}
	if got, want := serviceItem.ServiceId, apigen.ServiceId(101); got != want {
		t.Fatalf("serviceId = %d, want %d", got, want)
	}
}

func TestGetServiceReturnsNotFound(t *testing.T) {
	handler := testListHandler(t)

	res, err := handler.GetService(context.Background(), apigen.GetServiceParams{ID: 999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ErrorStatusCode); !ok {
		t.Fatalf("response type = %T, want *ErrorStatusCode", res)
	}
}

func TestApiServiceExposesEPGStatus(t *testing.T) {
	tests := []struct {
		name             string
		setup            func(context.Context, *service.ServiceManager, *testing.T)
		wantReady        bool
		wantAttempt      apigen.OptUnixtimeMS
		wantUpdated      apigen.OptUnixtimeMS
		wantLastError    apigen.OptString
		wantUpdatedIsSet bool
	}{
		{
			name:      "no attempt",
			wantReady: false,
		},
		{
			name: "latest failed attempt",
			setup: func(ctx context.Context, sm *service.ServiceManager, t *testing.T) {
				t.Helper()
				if err := sm.SetEPGAttempt(ctx, 1, 101, 1000, "boom"); err != nil {
					t.Fatal(err)
				}
				if err := sm.SetEPGAttempt(ctx, 1, 101, 2000, "still bad"); err != nil {
					t.Fatal(err)
				}
			},
			wantReady:     false,
			wantAttempt:   apigen.NewOptUnixtimeMS(2000),
			wantLastError: apigen.NewOptString("still bad"),
		},
		{
			name: "success",
			setup: func(ctx context.Context, sm *service.ServiceManager, t *testing.T) {
				t.Helper()
				if err := sm.SetEPGSuccess(ctx, 1, 101, 2000); err != nil {
					t.Fatal(err)
				}
			},
			wantReady:        true,
			wantAttempt:      apigen.NewOptUnixtimeMS(2000),
			wantUpdated:      apigen.NewOptUnixtimeMS(2000),
			wantUpdatedIsSet: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			database, err := db.OpenInMemory()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			store := service.NewSQLiteStore(database)
			if err := store.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
				{Id: "0000100101", ServiceId: 101, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
			}); err != nil {
				t.Fatal(err)
			}
			sm := service.NewServiceManager(store, config.ChannelsConfig{{Type: "GR", Channel: "27"}})
			if tt.setup != nil {
				tt.setup(ctx, sm, t)
			}
			handler := NewHandler(HandlerConfig{ServiceManager: sm})
			res, err := handler.GetServices(context.Background(), apigen.GetServicesParams{})
			if err != nil {
				t.Fatal(err)
			}
			services, ok := res.(*apigen.GetServicesOKApplicationJSON)
			if !ok {
				t.Fatalf("response type = %T, want *GetServicesOKApplicationJSON", res)
			}
			if len(*services) != 1 {
				t.Fatalf("services = %d, want 1", len(*services))
			}
			s := (*services)[0]
			if s.EpgReady.Value != tt.wantReady {
				t.Errorf("EpgReady = %v, want %v", s.EpgReady.Value, tt.wantReady)
			}
			if s.EpgLastAttemptAt != tt.wantAttempt {
				t.Errorf("EpgLastAttemptAt = %#v, want %#v", s.EpgLastAttemptAt, tt.wantAttempt)
			}
			if s.EpgUpdatedAt != tt.wantUpdated || s.EpgUpdatedAt.IsSet() != tt.wantUpdatedIsSet {
				t.Errorf("EpgUpdatedAt = %#v, want %#v", s.EpgUpdatedAt, tt.wantUpdated)
			}
			if s.EpgLastError != tt.wantLastError {
				t.Errorf("EpgLastError = %#v, want %#v", s.EpgLastError, tt.wantLastError)
			}
		})
	}
}

func TestApiServiceExposesMirakurunLogoFieldsAndImage(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store := service.NewSQLiteStore(database)
	logoID := int64(42)
	logoVersion := int64(3)
	downloadDataID := int64(0x1234)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "logo", LogoId: &logoID, LogoVersion: &logoVersion, LogoDownloadDataId: &downloadDataID, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	sm := service.NewServiceManager(store, config.ChannelsConfig{{Type: "GR", Channel: "27"}})
	handler := NewHandler(HandlerConfig{ServiceManager: sm})

	res, err := handler.GetServices(ctx, apigen.GetServicesParams{})
	if err != nil {
		t.Fatal(err)
	}
	services := res.(*apigen.GetServicesOKApplicationJSON)
	if (*services)[0].LogoId.Value != 42 {
		t.Fatalf("LogoId = %d, want 42", (*services)[0].LogoId.Value)
	}
	serviceType := reflect.TypeOf((*services)[0])
	if _, ok := serviceType.FieldByName("LogoVersion"); ok {
		t.Fatal("Service exposes internal LogoVersion field")
	}
	if _, ok := serviceType.FieldByName("LogoDownloadDataId"); ok {
		t.Fatal("Service exposes internal LogoDownloadDataId field")
	}
	if (*services)[0].HasLogoData.Value {
		t.Fatal("HasLogoData = true before image is stored")
	}
	unavailable, err := handler.GetLogoImage(ctx, apigen.GetLogoImageParams{ID: 100101})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := unavailable.(*apigen.GetLogoImageServiceUnavailable); !ok {
		t.Fatalf("logo response = %T, want service unavailable", unavailable)
	}

	smallData := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00}
	if err := sm.UpsertLogo(ctx, 1, 0, 101, logoID, 0, logoVersion, downloadDataID, smallData, 1234); err != nil {
		t.Fatal(err)
	}
	data := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x05}
	if err := sm.UpsertLogo(ctx, 1, 0, 101, logoID, 5, logoVersion, downloadDataID, data, 1234); err != nil {
		t.Fatal(err)
	}
	res, err = handler.GetServices(ctx, apigen.GetServicesParams{})
	if err != nil {
		t.Fatal(err)
	}
	services = res.(*apigen.GetServicesOKApplicationJSON)
	if !(*services)[0].HasLogoData.Value {
		t.Fatal("HasLogoData = false after image is stored")
	}
	imageRes, err := handler.GetLogoImage(ctx, apigen.GetLogoImageParams{ID: 100101})
	if err != nil {
		t.Fatal(err)
	}
	image, ok := imageRes.(*apigen.GetLogoImageOK)
	if !ok {
		t.Fatalf("logo response = %T, want OK", imageRes)
	}
	got, err := io.ReadAll(image)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("logo data = %v, want %v", got, data)
	}
}

func TestGetLogoImageReturnsNotFound(t *testing.T) {
	handler := testListHandler(t)
	res, err := handler.GetLogoImage(context.Background(), apigen.GetLogoImageParams{ID: 999999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.GetLogoImageNotFound); !ok {
		t.Fatalf("response = %T, want not found", res)
	}
}

func apiServiceNames(services []apigen.Service) []string {
	names := make([]string, len(services))
	for i, service := range services {
		names[i] = service.Name
	}
	return names
}
