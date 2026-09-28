package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"reflect"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/model"
)

func TestServiceManagerGetChannelsExcludesDisabledChannels(t *testing.T) {
	no := false
	yes := true
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	manager := NewManager(NewSQLiteStore(database), config.ChannelsConfig{
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

func TestServiceManagerGetServiceByIdPrefersExactIDOverItemID(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "100101", Service: model.Service{Key: model.ServiceKey{ServiceID: 102, NetworkID: 1}, Name: "exact"}, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100101", Service: model.Service{Key: model.ServiceKey{ServiceID: 101, NetworkID: 1}, Name: "item"}, ChannelType: "GR", ChannelId: "27"},
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
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	service := &Service{Id: "0000100101", Service: model.Service{Key: model.ServiceKey{ServiceID: 101, NetworkID: 1}, Name: "NHK"}}
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
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "100101", Service: model.Service{Key: model.ServiceKey{ServiceID: 102, NetworkID: 1}, Name: "exact"}, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100101", Service: model.Service{Key: model.ServiceKey{ServiceID: 101, NetworkID: 1}, Name: "item"}, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "101", []*Service{
		{Id: "bs", Service: model.Service{Key: model.ServiceKey{ServiceID: 101, NetworkID: 1}, Name: "other channel"}, ChannelType: "BS", ChannelId: "101"},
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
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	for _, channel := range []ChannelKey{{Type: "GR", ID: "27"}, {Type: "GR", ID: "28"}, {Type: "BS", ID: "101"}} {
		service := &Service{Id: channel.Type + channel.ID, Service: model.Service{Name: channel.ID}}
		if err := store.ReplaceChannelServices(ctx, channel.Type, channel.ID, []*Service{service}); err != nil {
			t.Fatal(err)
		}
	}
	disabled := true
	manager := NewManager(store, config.ChannelsConfig{
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
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", Service: model.Service{Key: model.ServiceKey{ServiceID: 101, NetworkID: 1}, Name: "NHK"}, ChannelType: "GR", ChannelId: "27"},
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
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", Service: model.Service{Key: model.ServiceKey{ServiceID: 101, NetworkID: 1}}, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100102", Service: model.Service{Key: model.ServiceKey{ServiceID: 102, NetworkID: 1}}, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100103", Service: model.Service{Key: model.ServiceKey{ServiceID: 103, NetworkID: 1}}, ChannelType: "GR", ChannelId: "27"},
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

// TestServiceManagerUpsertLogoImageStoresSessionData stores the PNG as the
// session delivered it: sessions complete the ARIB palette.
func TestServiceManagerUpsertLogoImageStoresSessionData(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	logoID := int64(42)
	logoVersion := int64(3)
	downloadDataID := int64(0x1234)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{{
		Id: "0000100101",
		Service: model.Service{
			Key:  model.ServiceKey{ServiceID: 101, NetworkID: 1},
			Name: "logo",
			Logo: &model.LogoRef{LogoID: uint16(logoID), Version: new(uint16(logoVersion)), DownloadDataID: new(uint16(downloadDataID))},
		},
		ChannelType: "GR",
		ChannelId:   "27",
	}}); err != nil {
		t.Fatal(err)
	}

	raw := buildServiceTestPalettePNG(true)
	if err := manager.UpsertLogoImage(ctx, model.Logo{
		NetworkID:      1,
		LogoID:         uint16(logoID),
		Version:        uint16(logoVersion),
		DownloadDataID: uint16(downloadDataID),
		LogoType:       5,
		Data:           raw,
	}); err != nil {
		t.Fatal(err)
	}

	stored, err := store.GetLogoByServiceItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, raw) {
		t.Fatal("stored logo data differs from the delivered PNG")
	}
}

func buildServiceTestPalettePNG(includePLTE bool) []byte {
	var png []byte
	png = append(png, []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}...)
	ihdr := make([]byte, 13)
	binary.BigEndian.PutUint32(ihdr[0:4], 1)
	binary.BigEndian.PutUint32(ihdr[4:8], 1)
	ihdr[8] = 8
	ihdr[9] = 3
	png = appendServiceTestPNGChunk(png, "IHDR", ihdr)
	if includePLTE {
		png = appendServiceTestPNGChunk(png, "PLTE", []byte{0, 0, 0})
	}
	png = appendServiceTestPNGChunk(png, "IDAT", []byte{0x78, 0x9c, 0x63, 0x60, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01})
	png = appendServiceTestPNGChunk(png, "IEND", nil)
	return png
}

func appendServiceTestPNGChunk(dst []byte, chunkType string, chunkData []byte) []byte {
	var scratch [4]byte
	binary.BigEndian.PutUint32(scratch[:], uint32(len(chunkData)))
	dst = append(dst, scratch[:]...)
	dst = append(dst, chunkType...)
	dst = append(dst, chunkData...)
	crc := crc32.NewIEEE()
	_, _ = crc.Write([]byte(chunkType))
	_, _ = crc.Write(chunkData)
	binary.BigEndian.PutUint32(scratch[:], crc.Sum32())
	dst = append(dst, scratch[:]...)
	return dst
}

func TestServiceManagerUpsertLogoImageRequiresSDTConsistency(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	logoID := int64(42)
	logoVersion := int64(3)
	downloadDataID := int64(0x1234)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{{
		Id: "0000100101",
		Service: model.Service{
			Key:  model.ServiceKey{ServiceID: 101, NetworkID: 1},
			Logo: &model.LogoRef{LogoID: uint16(logoID), Version: new(uint16(logoVersion)), DownloadDataID: new(uint16(downloadDataID))},
		},
		ChannelType: "GR",
		ChannelId:   "27",
	}}); err != nil {
		t.Fatal(err)
	}

	data := buildServiceTestPalettePNG(true)
	if err := manager.UpsertLogoImage(ctx, model.Logo{
		NetworkID:      1,
		LogoID:         42,
		Version:        4,
		DownloadDataID: 0x1234,
		LogoType:       5,
		Data:           data,
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

	if err := manager.UpsertLogoImage(ctx, model.Logo{
		NetworkID:      1,
		LogoID:         42,
		Version:        3,
		DownloadDataID: 0x1234,
		LogoType:       5,
		Data:           data,
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
	if err := manager.UpsertLogoImage(ctx, model.Logo{
		NetworkID:      1,
		LogoID:         42,
		Version:        3,
		DownloadDataID: 0x1234,
		LogoType:       5,
		Deleted:        true,
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

func TestSQLiteStorePreservesLogoRowsWhenServiceLogoMetadataChanges(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	logoID := int64(42)
	oldVersion := int64(3)
	newVersion := int64(4)
	downloadDataID := int64(0x1234)
	service := &Service{
		Id: "0000100101",
		Service: model.Service{
			Key:  model.ServiceKey{ServiceID: 101, NetworkID: 1},
			Logo: &model.LogoRef{LogoID: uint16(logoID), Version: new(uint16(oldVersion)), DownloadDataID: new(uint16(downloadDataID))},
		},
		ChannelType: "GR",
		ChannelId:   "27",
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 1, service.Key.StreamID, 101, logoID, 5, oldVersion, downloadDataID, []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}, 1000); err != nil {
		t.Fatal(err)
	}
	service.Logo.Version = new(uint16(newVersion))
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	svc, err := store.GetByItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if svc.HasLogoData {
		t.Fatal("HasLogoData = true after service logo version changed")
	}
	service.Logo.Version = new(uint16(oldVersion))
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	svc, err = store.GetByItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if !svc.HasLogoData {
		t.Fatal("HasLogoData = false after restoring service logo metadata")
	}
}

func TestSQLiteStorePreservesExistingLogoMetadataWhenScanOmitsLogo(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	logoID := int64(42)
	logoVersion := int64(3)
	downloadDataID := int64(0x1234)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*Service{{
		Id: "0000400101",
		Service: model.Service{
			Key:  model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101},
			Logo: &model.LogoRef{LogoID: uint16(logoID), Version: new(uint16(logoVersion)), DownloadDataID: new(uint16(downloadDataID))},
		},
		ChannelType: "BS",
		ChannelId:   "BS01",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0x4010, 101, logoID, 5, logoVersion, downloadDataID, []byte("png"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*Service{{
		Id: "0000400101",
		Service: model.Service{
			Key: model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101},
		},
		ChannelType: "BS",
		ChannelId:   "BS01",
	}}); err != nil {
		t.Fatal(err)
	}
	svc, err := store.GetByItemID(ctx, 400101)
	if err != nil {
		t.Fatal(err)
	}
	if !logoMatches(svc.Logo, logoID, logoVersion, downloadDataID) || !svc.HasLogoData {
		t.Fatalf("service logo metadata = %#v, want preserved metadata and data", svc)
	}
}

func TestHasLogoDataTracksExactStoredVersion(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	logoID, version, downloadID := int64(42), int64(3), int64(7)
	service := &Service{
		Id: "0000100101",
		Service: model.Service{
			Key:  model.ServiceKey{NetworkID: 1, ServiceID: 101, StreamID: 10},
			Logo: &model.LogoRef{LogoID: uint16(logoID), Version: new(uint16(version)), DownloadDataID: new(uint16(downloadID))},
		},
		ChannelType: "GR",
		ChannelId:   "27",
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetByItemID(ctx, 100101); err != nil || got.HasLogoData {
		t.Fatalf("service before upsert = %#v, err=%v, want no logo data", got, err)
	}
	if err := store.UpsertLogo(ctx, 1, service.Key.StreamID, 101, logoID, 5, version-1, downloadID, []byte("old"), 1000); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetByItemID(ctx, 100101); err != nil || got.HasLogoData {
		t.Fatalf("service with another version stored = %#v, err=%v, want no logo data", got, err)
	}
	if err := store.UpsertLogo(ctx, 1, service.Key.StreamID, 101, logoID, 5, version, downloadID, []byte("png"), 1000); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetByItemID(ctx, 100101); err != nil || !got.HasLogoData {
		t.Fatalf("service after upsert = %#v, err=%v, want logo data", got, err)
	}
}

func TestCommonDataAnnouncementUpsertReplacesOlderRoute(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	announcement := model.CommonDataAnnouncement{Service: model.ServiceKey{NetworkID: 4, StreamID: 0x4031, ServiceID: 929}, DownloadID: 1, VersionID: 1}
	if err := manager.UpsertCommonDataAnnouncement(ctx, announcement, "sat", "old"); err != nil {
		t.Fatal(err)
	}
	announcement.DownloadID = 2
	announcement.VersionID = 2
	if err := manager.UpsertCommonDataAnnouncement(ctx, announcement, "sat", "new"); err != nil {
		t.Fatal(err)
	}
	got, err := store.ListCommonDataAnnouncements(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DownloadID != 2 || got[0].VersionID != 2 || got[0].ObservedChannelID != "new" {
		t.Fatalf("announcements = %#v, want replaced row", got)
	}
}

func TestServiceManagerUpsertLogoImageAssignsNamedServicesByTSID(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "sat", "a", []*Service{
		{Id: "0000400101", Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101}}, ChannelType: "sat", ChannelId: "a"},
		{Id: "0000400102", Service: model.Service{Key: model.ServiceKey{NetworkID: 4, StreamID: 0x4020, ServiceID: 102}}, ChannelType: "sat", ChannelId: "a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.UpsertLogoImage(ctx, model.Logo{
		NetworkID: 4, LogoID: 12, LogoType: 5, Version: 2, DownloadDataID: 0x1234, Data: buildServiceTestPalettePNG(true),
		Services: []model.ServiceKey{{NetworkID: 4, StreamID: 0x4010, ServiceID: 101}},
	}); err != nil {
		t.Fatal(err)
	}
	matched, err := store.GetByItemID(ctx, 400101)
	if err != nil {
		t.Fatal(err)
	}
	if matched.Logo == nil || matched.Logo.LogoID != 12 || !matched.HasLogoData {
		t.Fatalf("matched service = %#v, want common logo metadata and data", matched)
	}
	unmatched, err := store.GetByItemID(ctx, 400102)
	if err != nil {
		t.Fatal(err)
	}
	if unmatched.HasLogoData || unmatched.Logo != nil {
		t.Fatalf("unmatched service = %#v, want no logo", unmatched)
	}
}

func TestServiceManagerUpsertAssignedLogoKeepsOldMetadataWhenLogoStoreFails(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	oldLogoID, oldVersion, oldDownloadID := int64(11), int64(1), int64(0x1111)
	if err := store.ReplaceChannelServices(ctx, "sat", "a", []*Service{{
		Id: "0000400101",
		Service: model.Service{
			Key:  model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101},
			Logo: &model.LogoRef{LogoID: uint16(oldLogoID), Version: new(uint16(oldVersion)), DownloadDataID: new(uint16(oldDownloadID))},
		},
		ChannelType: "sat",
		ChannelId:   "a",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0x4010, 101, oldLogoID, 5, oldVersion, oldDownloadID, buildServiceTestPalettePNG(true), 1000); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(failingLogoStore{Store: store, err: errors.New("store failed")}, config.ChannelsConfig{})

	err = manager.UpsertLogoImage(ctx, model.Logo{
		NetworkID: 4, LogoID: 12, LogoType: 5, Version: 2, DownloadDataID: 0x2222, Data: buildServiceTestPalettePNG(true),
		Services: []model.ServiceKey{{NetworkID: 4, StreamID: 0x4010, ServiceID: 101}},
	})
	if err == nil {
		t.Fatal("UpsertLogoImage error = nil, want store failure")
	}
	svc, err := store.GetByItemID(ctx, 400101)
	if err != nil {
		t.Fatal(err)
	}
	if !logoMatches(svc.Logo, oldLogoID, oldVersion, oldDownloadID) || !svc.HasLogoData {
		t.Fatalf("service logo metadata = %#v, want old metadata and logo data preserved", svc)
	}
}

type failingLogoStore struct {
	Store
	err error
}

func (s failingLogoStore) UpsertLogo(context.Context, uint16, uint16, uint16, int64, int64, int64, int64, []byte, int64) error {
	return s.err
}

func logoMatches(logo *model.LogoRef, logoID, version, downloadDataID int64) bool {
	return logo != nil && int64(logo.LogoID) == logoID &&
		logo.Version != nil && int64(*logo.Version) == version &&
		logo.DownloadDataID != nil && int64(*logo.DownloadDataID) == downloadDataID
}

func TestSQLiteStoreRoundTripsBroadcastService(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	withKey := &Service{
		Id: "0000100101",
		Service: model.Service{
			Key:              model.ServiceKey{NetworkID: 1, StreamID: 10, ServiceID: 101},
			Name:             "NHK",
			ProviderName:     "provider",
			Type:             1,
			RunningStatus:    4,
			FreeCA:           true,
			EITSchedule:      true,
			RemoteControlKey: new(uint8(1)),
			Logo:             &model.LogoRef{LogoID: 3, SimpleLogo: "NHK", HasSimpleLogo: true},
		},
		ChannelType: "GR",
		ChannelId:   "27",
	}
	withoutKey := &Service{
		Id:          "0000100102",
		Service:     model.Service{Key: model.ServiceKey{NetworkID: 1, StreamID: 10, ServiceID: 102}, Name: "no key"},
		ChannelType: "GR",
		ChannelId:   "27",
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{withKey, withoutKey}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []*Service{withKey, withoutKey} {
		got, err := store.GetByID(ctx, want.Id)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Service, want.Service) {
			t.Errorf("service %s = %#v, want %#v", want.Id, got.Service, want.Service)
		}
	}
}

func TestSQLiteStoreDoesNotAttachOldCDTLogoToSimpleLogo(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	svc := &Service{
		Id: "0000100101",
		Service: model.Service{
			Key:  model.ServiceKey{NetworkID: 1, StreamID: 10, ServiceID: 101},
			Logo: &model.LogoRef{LogoID: 3, Version: new(uint16(2)), DownloadDataID: new(uint16(7))},
		},
		ChannelType: "GR",
		ChannelId:   "27",
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{svc}); err != nil {
		t.Fatal(err)
	}
	svc.Logo = &model.LogoRef{SimpleLogo: "ＮＨＫ", HasSimpleLogo: true}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{svc}); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetByID(ctx, svc.Id)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Logo, svc.Logo) {
		t.Fatalf("logo = %#v, want %#v", got.Logo, svc.Logo)
	}
}
