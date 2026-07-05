package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/ts"
)

func TestServiceManagerGetChannelsExcludesDisabledChannels(t *testing.T) {
	no := false
	yes := true
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
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

func TestServiceManagerGetServiceByIdPrefersExactIDOverItemID(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
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
	defer func() { _ = database.Close() }()
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
	defer func() { _ = database.Close() }()
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
	defer func() { _ = database.Close() }()
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
	defer func() { _ = database.Close() }()
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
	defer func() { _ = database.Close() }()
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

func TestServiceManagerUpsertLogoImageNormalizesARIBPNG(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	logoID := int64(42)
	logoVersion := int64(3)
	downloadDataID := int64(0x1234)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{{
		Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "logo",
		LogoId: &logoID, LogoVersion: &logoVersion, LogoDownloadDataId: &downloadDataID,
		ChannelType: "GR", ChannelId: "27",
	}}); err != nil {
		t.Fatal(err)
	}

	raw := buildServiceTestPalettePNG(false)
	if err := manager.UpsertLogoImage(ctx, &ts.LogoImage{
		OriginalNetworkID: 1,
		LogoID:            uint16(logoID),
		LogoVersion:       uint16(logoVersion),
		DownloadDataID:    uint16(downloadDataID),
		LogoType:          5,
		Data:              raw,
	}); err != nil {
		t.Fatal(err)
	}

	stored, err := store.GetLogoByServiceItemID(ctx, 100101)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(stored, raw) {
		t.Fatal("stored logo data was not normalized")
	}
	if !serviceTestPNGHasChunk(stored, "PLTE") {
		t.Fatal("stored logo data does not include PLTE")
	}
	if !serviceTestPNGHasChunk(stored, "tRNS") {
		t.Fatal("stored logo data does not include tRNS")
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

func serviceTestPNGHasChunk(png []byte, wantType string) bool {
	pos := 8
	for pos+12 <= len(png) {
		chunkLen := int(binary.BigEndian.Uint32(png[pos : pos+4]))
		chunkEnd := pos + 8 + chunkLen + 4
		if chunkEnd > len(png) {
			return false
		}
		if string(png[pos+4:pos+8]) == wantType {
			return true
		}
		pos = chunkEnd
	}
	return false
}

func TestServiceManagerUpsertLogoImageRequiresSDTConsistency(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
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

	data := buildServiceTestPalettePNG(true)
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
	if err := store.UpsertLogo(ctx, 1, service.TransportStreamId, 101, logoID, 5, oldVersion, downloadDataID, []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}, 1000); err != nil {
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
		t.Fatal("HasLogoData = true after service logo version changed")
	}
	service.LogoVersion = &oldVersion
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
		Id: "0000400101", NetworkId: 4, TransportStreamId: 0x4010, ServiceId: 101,
		LogoId: &logoID, LogoVersion: &logoVersion, LogoDownloadDataId: &downloadDataID,
		ChannelType: "BS", ChannelId: "BS01",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0x4010, 101, logoID, 5, logoVersion, downloadDataID, []byte("png"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*Service{{
		Id: "0000400101", NetworkId: 4, TransportStreamId: 0x4010, ServiceId: 101,
		ChannelType: "BS", ChannelId: "BS01",
	}}); err != nil {
		t.Fatal(err)
	}
	svc, err := store.GetByItemID(ctx, 400101)
	if err != nil {
		t.Fatal(err)
	}
	if svc.LogoId == nil || *svc.LogoId != logoID || svc.LogoVersion == nil || *svc.LogoVersion != logoVersion ||
		svc.LogoDownloadDataId == nil || *svc.LogoDownloadDataId != downloadDataID || !svc.HasLogoData {
		t.Fatalf("service logo metadata = %#v, want preserved metadata and data", svc)
	}
}

func TestMissingLogoTargetsTracksExactStoredVersion(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	logoID, version, downloadID := int64(42), int64(3), int64(7)
	service := &Service{
		Id: "0000100101", NetworkId: 1, ServiceId: 101, TransportStreamId: 10,
		ChannelType: "GR", ChannelId: "27", LogoId: &logoID, LogoVersion: &version, LogoDownloadDataId: &downloadID,
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	missing, err := store.MissingLogoTargets(ctx)
	if err != nil || len(missing) != 1 {
		t.Fatalf("missing before upsert = %#v, err=%v", missing, err)
	}
	if err := store.UpsertLogo(ctx, 1, service.TransportStreamId, 101, logoID, 5, version, downloadID, []byte("png"), 1000); err != nil {
		t.Fatal(err)
	}
	missing, err = store.MissingLogoTargets(ctx)
	if err != nil || len(missing) != 0 {
		t.Fatalf("missing after upsert = %#v, err=%v", missing, err)
	}
}

func TestLogoGatherTargetsRefreshesKnownLogos(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)

	remoteLogoID, remoteVersion, remoteDownloadID := int64(12), int64(0), int64(101)
	localLogoID, localVersion, localDownloadID := int64(13), int64(3), int64(7)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000400101", NetworkId: 4, ServiceId: 101, ChannelType: "GR", ChannelId: "27", LogoId: &remoteLogoID, LogoVersion: &remoteVersion, LogoDownloadDataId: &remoteDownloadID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*Service{
		{Id: "0000400102", NetworkId: 4, ServiceId: 102, ChannelType: "BS", ChannelId: "BS01", LogoId: &localLogoID, LogoVersion: &localVersion, LogoDownloadDataId: &localDownloadID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0, 101, remoteLogoID, 5, remoteVersion, remoteDownloadID, []byte("remote"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0, 102, localLogoID, 5, localVersion, localDownloadID, []byte("local"), 1000); err != nil {
		t.Fatal(err)
	}

	no := false
	manager := NewServiceManager(store, config.ChannelsConfig{
		{Name: "Remote", Type: "GR", Channel: "27", IsDisabled: &no, Routes: []config.ChannelRouteConfig{{Remote: "mirakurun", Type: "GR", Channel: "27", IsDisabled: &no}}},
		{Name: "Local", Type: "BS", Channel: "BS01", IsDisabled: &no},
	})

	missing, err := manager.MissingLogoTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing targets = %#v, want none", missing)
	}
	targets, err := manager.LogoGatherTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("logo gather targets = %#v, want known logo targets", targets)
	}
	if !hasLogoTarget(targets, "GR", "27", remoteLogoID, false) {
		t.Fatalf("logo gather target = %#v, want remote synthetic target", targets[0])
	}
	if !hasLogoTarget(targets, "BS", "BS01", localLogoID, false) {
		t.Fatalf("logo gather targets = %#v, want local known target", targets)
	}
}

func TestLogoGatherTargetsUsesONIDForCommonDataInsteadOfChannelType(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(ctx, "anything", "sat-a", []*Service{{
		Id: "0000400101", NetworkId: 4, TransportStreamId: 0x4010, ServiceId: 101,
		ChannelType: "anything", ChannelId: "sat-a",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "not-satellite", []*Service{{
		Id: "1234500101", NetworkId: 12345, TransportStreamId: 0x2222, ServiceId: 101,
		ChannelType: "BS", ChannelId: "not-satellite",
	}}); err != nil {
		t.Fatal(err)
	}
	manager := NewServiceManager(store, config.ChannelsConfig{})
	targets, err := manager.LogoGatherTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets = %#v, want one satellite common-data target", targets)
	}
	if !targets[0].IsCommonData || !targets[0].IsSDTTProbe || targets[0].ChannelType != "anything" || targets[0].NetworkId != 4 {
		t.Fatalf("target = %#v, want ONID-based common-data target", targets[0])
	}
}

func TestLogoGatherTargetsUsesSDTTAnnouncementChannel(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(ctx, "sat", "target", []*Service{{
		Id: "0000400101", NetworkId: 4, TransportStreamId: 0x4010, ServiceId: 101,
		ChannelType: "sat", ChannelId: "target",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "sat", "common", []*Service{{
		Id: "0000492900", NetworkId: 4, TransportStreamId: 0x4031, ServiceId: 929,
		ChannelType: "sat", ChannelId: "common",
	}}); err != nil {
		t.Fatal(err)
	}
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := manager.UpsertCommonDataAnnouncement(ctx, ts.CommonDataAnnouncement{
		OriginalNetworkID: 4, TransportStreamID: 0x4031, ServiceID: 929, DownloadID: 0x12345678, VersionID: 7,
	}, "sat", "target"); err != nil {
		t.Fatal(err)
	}
	targets, err := manager.LogoGatherTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets = %#v, want one common-data target", targets)
	}
	if !targets[0].IsCommonData || targets[0].IsSDTTProbe || targets[0].ChannelType != "sat" || targets[0].ChannelId != "common" {
		t.Fatalf("target = %#v, want SDTT common-data channel", targets[0])
	}
}

func TestLogoGatherTargetsRefreshesCommonDataWhenLogosArePresent(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	logoID, logoVersion, downloadID := int64(12), int64(3), int64(7)
	if err := store.ReplaceChannelServices(ctx, "sat", "service", []*Service{{
		Id: "0000400101", NetworkId: 4, TransportStreamId: 0x4010, ServiceId: 101,
		LogoId: &logoID, LogoVersion: &logoVersion, LogoDownloadDataId: &downloadID,
		ChannelType: "sat", ChannelId: "service",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0x4010, 101, logoID, 5, logoVersion, downloadID, []byte("png"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "sat", "common", []*Service{{
		Id: "0000492900", NetworkId: 4, TransportStreamId: 0x40f1, ServiceId: 929,
		ChannelType: "sat", ChannelId: "common",
	}}); err != nil {
		t.Fatal(err)
	}
	manager := NewServiceManager(store, config.ChannelsConfig{})

	missing, err := manager.MissingLogoTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing targets = %#v, want none", missing)
	}
	targets, err := manager.LogoGatherTargets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %#v, want known logo and common-data refresh targets", targets)
	}
	if !hasLogoTarget(targets, "sat", "service", logoID, false) {
		t.Fatalf("targets = %#v, want known service logo refresh target", targets)
	}
	if !hasLogoTarget(targets, "sat", "common", 0, true) {
		t.Fatalf("target = %#v, want common-data refresh target", targets[0])
	}
}

func hasLogoTarget(targets []LogoTarget, channelType, channelID string, logoID int64, common bool) bool {
	for _, target := range targets {
		if target.ChannelType == channelType && target.ChannelId == channelID && target.LogoId == logoID && target.IsCommonData == common {
			return true
		}
	}
	return false
}

func TestCommonDataAnnouncementUpsertReplacesOlderRoute(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	announcement := ts.CommonDataAnnouncement{OriginalNetworkID: 4, TransportStreamID: 0x4031, ServiceID: 929, DownloadID: 1, VersionID: 1}
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

func TestServiceManagerUpsertCommonLogoImageUpdatesServiceByTSID(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "sat", "a", []*Service{
		{Id: "0000400101", NetworkId: 4, TransportStreamId: 0x4010, ServiceId: 101, ChannelType: "sat", ChannelId: "a"},
		{Id: "0000400102", NetworkId: 4, TransportStreamId: 0x4020, ServiceId: 102, ChannelType: "sat", ChannelId: "a"},
	}); err != nil {
		t.Fatal(err)
	}
	raw := buildServiceTestPalettePNG(false)
	if err := manager.UpsertCommonLogoImage(ctx, ts.CommonLogoImage{
		LogoID: 12, LogoType: 5, LogoVersion: 2, DownloadID: 0x1234, Data: raw,
		Services: []ts.CommonLogoService{{OriginalNetworkID: 4, TransportStreamID: 0x4010, ServiceID: 101}},
	}); err != nil {
		t.Fatal(err)
	}
	matched, err := store.GetByItemID(ctx, 400101)
	if err != nil {
		t.Fatal(err)
	}
	if matched.LogoId == nil || *matched.LogoId != 12 || !matched.HasLogoData {
		t.Fatalf("matched service = %#v, want common logo metadata and data", matched)
	}
	unmatched, err := store.GetByItemID(ctx, 400102)
	if err != nil {
		t.Fatal(err)
	}
	if unmatched.HasLogoData || unmatched.LogoId != nil {
		t.Fatalf("unmatched service = %#v, want no logo", unmatched)
	}
}

func TestServiceManagerUpsertCommonLogoImageKeepsOldMetadataWhenLogoStoreFails(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := NewSQLiteStore(database)
	oldLogoID, oldVersion, oldDownloadID := int64(11), int64(1), int64(0x1111)
	if err := store.ReplaceChannelServices(ctx, "sat", "a", []*Service{{
		Id: "0000400101", NetworkId: 4, TransportStreamId: 0x4010, ServiceId: 101,
		LogoId: &oldLogoID, LogoVersion: &oldVersion, LogoDownloadDataId: &oldDownloadID,
		ChannelType: "sat", ChannelId: "a",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0x4010, 101, oldLogoID, 5, oldVersion, oldDownloadID, buildServiceTestPalettePNG(true), 1000); err != nil {
		t.Fatal(err)
	}
	manager := NewServiceManager(failingLogoStore{Store: store, err: errors.New("store failed")}, config.ChannelsConfig{})

	err = manager.UpsertCommonLogoImage(ctx, ts.CommonLogoImage{
		LogoID: 12, LogoType: 5, LogoVersion: 2, DownloadID: 0x2222, Data: buildServiceTestPalettePNG(true),
		Services: []ts.CommonLogoService{{OriginalNetworkID: 4, TransportStreamID: 0x4010, ServiceID: 101}},
	})
	if err == nil {
		t.Fatal("UpsertCommonLogoImage error = nil, want store failure")
	}
	svc, err := store.GetByItemID(ctx, 400101)
	if err != nil {
		t.Fatal(err)
	}
	if svc.LogoId == nil || *svc.LogoId != oldLogoID || svc.LogoVersion == nil || *svc.LogoVersion != oldVersion ||
		svc.LogoDownloadDataId == nil || *svc.LogoDownloadDataId != oldDownloadID || !svc.HasLogoData {
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
