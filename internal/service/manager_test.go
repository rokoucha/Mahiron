package service

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/ts"
)

func TestServiceManagerGetChannelsExcludesDisabledChannels(t *testing.T) {
	no := false
	yes := true
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	manager := NewServiceManager(NewSQLiteStore(database), config.ChannelsConfig{
		{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no},
		{Name: "Disabled", Type: "GR", Channel: "28", IsDisabled: &yes},
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
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})

	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{
			Id:          "0000100101",
			ServiceId:   101,
			NetworkId:   1,
			Name:        "NHK",
			ChannelType: "GR",
			ChannelId:   "27",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "101", []*Service{
		{
			Id:          "0000200102",
			ServiceId:   102,
			NetworkId:   2,
			Name:        "BS",
			ChannelType: "BS",
			ChannelId:   "101",
		},
	}); err != nil {
		t.Fatal(err)
	}

	services, err := manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(services), 2; got != want {
		t.Fatalf("services length = %d, want %d", got, want)
	}

	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{
			Id:          "0000100101",
			ServiceId:   101,
			NetworkId:   1,
			Name:        "NHK Updated",
			ChannelType: "GR",
			ChannelId:   "27",
		},
	}); err != nil {
		t.Fatal(err)
	}

	services, err = manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(services), 2; got != want {
		t.Fatalf("services length after update = %d, want %d", got, want)
	}
	svc, err := manager.GetServiceById(ctx, "100101")
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil {
		t.Fatal("service not found")
	}
	if got, want := svc.Name, "NHK Updated"; got != want {
		t.Fatalf("updated service name = %q, want %q", got, want)
	}
}

func TestServiceManagerGetServiceByIdPrefersExactIDOverItemID(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "100101", ServiceId: 102, NetworkId: 1, Name: "exact", ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "item", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}

	svc, err := manager.GetServiceById(ctx, "100101")
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil || svc.Name != "exact" {
		t.Fatalf("service = %#v, want exact ID match", svc)
	}

	svc, err = manager.GetServiceById(ctx, "100102")
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil || svc.Name != "exact" {
		t.Fatalf("service = %#v, want ItemId fallback match", svc)
	}
}

func TestSQLiteStoreMovesServiceBetweenChannels(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	service := &Service{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "NHK"}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "28", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	old, err := store.GetByChannel(ctx, "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	moved, err := store.GetByChannel(ctx, "GR", "28")
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 0 || len(moved) != 1 {
		t.Fatalf("old=%d moved=%d, want old=0 moved=1", len(old), len(moved))
	}
}

func TestServiceManagerGetServiceByChannelAndIdPrefersExactIDOverItemID(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "100101", ServiceId: 102, NetworkId: 1, Name: "exact", ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "item", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "101", []*Service{
		{Id: "bs", ServiceId: 101, NetworkId: 1, Name: "other channel", ChannelType: "BS", ChannelId: "101"},
	}); err != nil {
		t.Fatal(err)
	}

	svc, err := manager.GetServiceByChannelAndId(ctx, "GR", "27", "100101")
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil || svc.Name != "exact" {
		t.Fatalf("service = %#v, want exact ID match", svc)
	}

	svc, err = manager.GetServiceByChannelAndId(ctx, "GR", "27", "100102")
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil || svc.Name != "exact" {
		t.Fatalf("service = %#v, want ItemId fallback match in channel", svc)
	}
}

func TestServiceManagerReconcileChannelsPrunesRemovedAndDisabled(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	for _, channel := range []ChannelKey{{Type: "GR", ID: "27"}, {Type: "GR", ID: "28"}, {Type: "BS", ID: "101"}} {
		service := &Service{Id: channel.Type + channel.ID, Name: channel.ID}
		if err := store.ReplaceChannelServices(ctx, channel.Type, channel.ID, []*Service{service}); err != nil {
			t.Fatal(err)
		}
	}
	disabled := true
	manager := NewServiceManager(store, config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "28", IsDisabled: &disabled},
	})
	if err := manager.ReconcileChannels(ctx); err != nil {
		t.Fatal(err)
	}
	services, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 1 || services[0].ChannelId != "27" {
		t.Fatalf("services = %#v, want only GR/27", services)
	}
}

func TestServiceManagerEPGStatus(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "NHK", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGAttempt(ctx, 1, 101, 1000, "boom"); err != nil {
		t.Fatal(err)
	}
	services, err := manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if services[0].EPG.LastError != "boom" {
		t.Fatalf("LastError = %q, want boom", services[0].EPG.LastError)
	}
	if services[0].EPG.LastAttemptAt == nil || *services[0].EPG.LastAttemptAt != 1000 {
		t.Fatalf("LastAttemptAt = %v, want 1000", services[0].EPG.LastAttemptAt)
	}
	if services[0].EPG.LastSuccessAt != nil {
		t.Fatalf("LastSuccessAt = %v, want nil", services[0].EPG.LastSuccessAt)
	}
	svc, err := manager.GetServiceById(ctx, "0000100101")
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil || svc.EPG.LastError != "boom" || svc.EPG.LastAttemptAt == nil || *svc.EPG.LastAttemptAt != 1000 {
		t.Fatalf("service by ID EPG = %#v, want joined EPG status", svc)
	}
	if err := manager.SetEPGSuccess(ctx, 1, 101, 2000); err != nil {
		t.Fatal(err)
	}
	services, err = manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if services[0].EPG.LastError != "" {
		t.Fatalf("LastError = %q, want empty", services[0].EPG.LastError)
	}
	if services[0].EPG.LastSuccessAt == nil || *services[0].EPG.LastSuccessAt != 2000 {
		t.Fatalf("LastSuccessAt = %v, want 2000", services[0].EPG.LastSuccessAt)
	}
}

func TestServiceManagerEPGSummary(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100102", ServiceId: 102, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100103", ServiceId: 103, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGSuccess(ctx, 1, 101, 1000); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGAttempt(ctx, 1, 102, 2000, "boom"); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGAttempt(ctx, 1, 103, 3000, ""); err != nil {
		t.Fatal(err)
	}
	stale, failed, lastSuccess, err := manager.EPGSummary(ctx, 500, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if stale != 3 {
		t.Errorf("stale = %d, want 3 (everything older than 500ms)", stale)
	}
	if failed != 1 {
		t.Errorf("failed = %d, want 1", failed)
	}
	if lastSuccess == nil || *lastSuccess != 1000 {
		t.Errorf("lastSuccess = %v, want 1000", lastSuccess)
	}
	stale, _, _, err = manager.EPGSummary(ctx, 5000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if stale != 2 {
		t.Errorf("stale = %d, want 2 with larger window", stale)
	}
}

func TestServiceManagerUpsertLogoImageRequiresSDTConsistency(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	logoID := int64(42)
	logoVersion := int64(3)
	downloadDataID := int64(0x1234)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{{
		Id:                 "0000100101",
		ServiceId:          101,
		NetworkId:          1,
		LogoId:             &logoID,
		LogoVersion:        &logoVersion,
		LogoDownloadDataId: &downloadDataID,
		ChannelType:        "GR",
		ChannelId:          "27",
	}}); err != nil {
		t.Fatal(err)
	}

	data := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}
	if err := manager.UpsertLogoImage(ctx, &ts.LogoImage{
		OriginalNetworkID: 1,
		LogoID:            42,
		LogoVersion:       4,
		DownloadDataID:    0x1234,
		LogoType:          5,
		Data:              data,
	}); err != nil {
		t.Fatal(err)
	}
	svc, err := manager.GetServiceByItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if svc.HasLogoData {
		t.Fatal("HasLogoData = true for mismatched logo version")
	}

	if err := manager.UpsertLogoImage(ctx, &ts.LogoImage{
		OriginalNetworkID: 1,
		LogoID:            42,
		LogoVersion:       3,
		DownloadDataID:    0x1234,
		LogoType:          5,
		Data:              data,
	}); err != nil {
		t.Fatal(err)
	}
	svc, err = manager.GetServiceByItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if !svc.HasLogoData {
		t.Fatal("HasLogoData = false for consistent logo metadata")
	}
	if err := manager.UpsertLogoImage(ctx, &ts.LogoImage{
		OriginalNetworkID: 1,
		LogoID:            42,
		LogoVersion:       3,
		DownloadDataID:    0x1234,
		LogoType:          5,
		IsDeleted:         true,
	}); err != nil {
		t.Fatal(err)
	}
	svc, err = manager.GetServiceByItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if svc.HasLogoData {
		t.Fatal("HasLogoData = true after CDT deletion notice")
	}
}

func TestSQLiteStoreDeletesStaleLogosWhenServiceLogoMetadataChanges(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	logoID := int64(42)
	oldVersion := int64(3)
	newVersion := int64(4)
	downloadDataID := int64(0x1234)
	service := &Service{
		Id:                 "0000100101",
		ServiceId:          101,
		NetworkId:          1,
		LogoId:             &logoID,
		LogoVersion:        &oldVersion,
		LogoDownloadDataId: &downloadDataID,
		ChannelType:        "GR",
		ChannelId:          "27",
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 1, 101, logoID, 5, oldVersion, downloadDataID, []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}, 1000); err != nil {
		t.Fatal(err)
	}
	service.LogoVersion = &newVersion
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	svc, err := store.GetByItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if svc.HasLogoData {
		t.Fatal("HasLogoData = true after service logo version changed and stale cache was deleted")
	}
}
