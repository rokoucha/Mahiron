package api

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/stream"
	"github.com/21S1298001/mahiron/internal/tuner"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func testStreamHeadHandler(t *testing.T) (*Handler, *service.ServiceManager) {
	t.Helper()
	no := false
	channels := config.ChannelsConfig{
		{Name: "Test", Type: "GR", Channel: "27", IsDisabled: &no},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	store := service.NewSQLiteStore(database)
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	if err := store.ReplaceChannelServices(context.Background(), "GR", "27", []*service.Service{
		{
			Id:                 "0000100001",
			ServiceId:          1,
			NetworkId:          1,
			Name:               "Test Service",
			Type:               1,
			RemoteControlKeyId: 1,
			ChannelType:        "GR",
			ChannelId:          "27",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := pm.ReplaceServicePrograms(context.Background(), 1, 1, 0, []*program.Program{
		{
			ID:        program.ProgramID(1, 1, 10),
			EventID:   10,
			ServiceID: 1,
			NetworkID: 1,
			StartAt:   1000,
			Duration:  1000,
			IsFree:    true,
			Name:      "Test Program",
		},
	}); err != nil {
		t.Fatal(err)
	}

	tunerManager := tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: config.TunersConfig{
			{Name: "first", Types: []string{"GR"}, Command: "sleep 30"},
		},
	})
	sm := service.NewServiceManager(store, channels)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{
		Channels:     channels,
		EITUpdater:   epg.NewUpdater(pm),
		TunerManager: tunerManager,
	})
	handler := NewHandler(HandlerConfig{
		ServiceManager: sm,
		ProgramManager: pm,
		StreamManager:  stm,
		TunerManager:   tunerManager,
	})
	return handler, sm
}

func TestServicesIDStreamHeadSuccess(t *testing.T) {
	handler, sm := testStreamHeadHandler(t)
	svc, err := sm.GetServicesByChannel(context.Background(), "GR", "27")
	if err != nil || len(svc) == 0 {
		t.Fatalf("GetServicesByChannel = %v, %v", svc, err)
	}
	itemID := svc[0].ItemId()
	res, err := handler.ServicesIDStreamHead(context.Background(), apigen.ServicesIDStreamHeadParams{
		ID:                 itemID,
		XMirakurunPriority: apigen.NewOptInt(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, isOK := res.(*apigen.ServicesIDStreamHeadOK)
	if !isOK {
		t.Fatalf("response type = %T, want *ServicesIDStreamHeadOK", res)
	}
	userID, set := ok.XMirakurunTunerUserID.Get()
	if !set || userID == "" {
		t.Fatal("X-Mirakurun-Tuner-User-ID should be set")
	}
}

func TestServicesIDStreamHeadNotFound(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	res, err := handler.ServicesIDStreamHead(context.Background(), apigen.ServicesIDStreamHeadParams{ID: 99999999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ServicesIDStreamHeadNotFound); !ok {
		t.Fatalf("response type = %T, want *ServicesIDStreamHeadNotFound", res)
	}
}

func TestChannelsTypeChannelStreamHeadSuccess(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	res, err := handler.ChannelsTypeChannelStreamHead(context.Background(), apigen.ChannelsTypeChannelStreamHeadParams{
		Type:               "GR",
		Channel:            "27",
		XMirakurunPriority: apigen.NewOptInt(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, isOK := res.(*apigen.ChannelsTypeChannelStreamHeadOK)
	if !isOK {
		t.Fatalf("response type = %T, want *ChannelsTypeChannelStreamHeadOK", res)
	}
	userID, set := ok.XMirakurunTunerUserID.Get()
	if !set || userID == "" {
		t.Fatal("X-Mirakurun-Tuner-User-ID should be set")
	}
}

func TestChannelsTypeChannelStreamHeadNotFound(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	res, err := handler.ChannelsTypeChannelStreamHead(context.Background(), apigen.ChannelsTypeChannelStreamHeadParams{
		Type: "GR", Channel: "999",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ChannelsTypeChannelStreamHeadNotFound); !ok {
		t.Fatalf("response type = %T, want *ChannelsTypeChannelStreamHeadNotFound", res)
	}
}

func TestChannelsTypeChannelServicesIDStreamHeadSuccess(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	res, err := handler.ChannelsTypeChannelServicesIDStreamHead(context.Background(), apigen.ChannelsTypeChannelServicesIDStreamHeadParams{
		Type: "GR", Channel: "27", ID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, isOK := res.(*apigen.ChannelsTypeChannelServicesIDStreamHeadOK)
	if !isOK {
		t.Fatalf("response type = %T, want *ChannelsTypeChannelServicesIDStreamHeadOK", res)
	}
	userID, set := ok.XMirakurunTunerUserID.Get()
	if !set || userID == "" {
		t.Fatal("X-Mirakurun-Tuner-User-ID should be set")
	}
}

func TestChannelsTypeChannelServicesIDStreamHeadNotFound(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	res, err := handler.ChannelsTypeChannelServicesIDStreamHead(context.Background(), apigen.ChannelsTypeChannelServicesIDStreamHeadParams{
		Type: "GR", Channel: "999", ID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ChannelsTypeChannelServicesIDStreamHeadNotFound); !ok {
		t.Fatalf("response type = %T, want *ChannelsTypeChannelServicesIDStreamHeadNotFound", res)
	}
}

func TestProgramsIDStreamHeadSuccess(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	res, err := handler.ProgramsIDStreamHead(context.Background(), apigen.ProgramsIDStreamHeadParams{
		ID: program.ProgramID(1, 1, 10),
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, isOK := res.(*apigen.ProgramsIDStreamHeadOK)
	if !isOK {
		t.Fatalf("response type = %T, want *ProgramsIDStreamHeadOK", res)
	}
	userID, set := ok.XMirakurunTunerUserID.Get()
	if !set || userID == "" {
		t.Fatal("X-Mirakurun-Tuner-User-ID should be set")
	}
}

func TestProgramsIDStreamHeadNotFound(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	res, err := handler.ProgramsIDStreamHead(context.Background(), apigen.ProgramsIDStreamHeadParams{ID: 99999999999999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ProgramsIDStreamHeadNotFound); !ok {
		t.Fatalf("response type = %T, want *ProgramsIDStreamHeadNotFound", res)
	}
}
