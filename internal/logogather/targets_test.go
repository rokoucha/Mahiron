package logogather

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

// testSources answers the common data questions as the TS sessions do.
type testSources struct {
	logos []model.Logo
	calls *int
}

// ObserveLogos reports the logos, then waits like a live channel does.
func (s testSources) ObserveLogos(ctx context.Context, _, _ string, observe func(model.Logo) error) error {
	if s.calls != nil {
		*s.calls++
	}
	for _, logo := range s.logos {
		if err := observe(logo); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (testSources) CommonDataNetwork(networkID uint16) bool {
	return isdb.IsSatelliteOriginalNetworkID(networkID)
}

func (testSources) DefaultCommonDataService() model.ServiceKey {
	return model.ServiceKey{NetworkID: 4, StreamID: 0x40f1, ServiceID: 929}
}

func TestTargetsRefreshesKnownLogos(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)

	remoteLogoID, remoteVersion, remoteDownloadID := int64(12), int64(0), int64(101)
	localLogoID, localVersion, localDownloadID := int64(13), int64(3), int64(7)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "0000400101", Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 101}, Logo: &model.LogoRef{LogoID: uint16(remoteLogoID), Version: new(uint16(remoteVersion)), DownloadDataID: new(uint16(remoteDownloadID))}}, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: "0000400102", Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 102}, Logo: &model.LogoRef{LogoID: uint16(localLogoID), Version: new(uint16(localVersion)), DownloadDataID: new(uint16(localDownloadID))}}, ChannelType: "BS", ChannelId: "BS01"},
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
	manager := service.NewManager(store, config.ChannelsConfig{
		{Name: "Remote", Type: "GR", Channel: "27", IsDisabled: &no, Routes: []config.ChannelRouteConfig{{Remote: "mirakurun", Type: "GR", Channel: "27", IsDisabled: &no}}},
		{Name: "Local", Type: "BS", Channel: "BS01", IsDisabled: &no},
	})

	targets, err := NewGatherer(manager, testSources{}, 0).Targets(ctx)
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

func TestTargetsUsesONIDForCommonDataInsteadOfChannelType(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(ctx, "anything", "sat-a", []*service.Service{{
		Id: "0000400101",
		Service: model.Service{
			Key: model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101},
		},
		ChannelType: "anything",
		ChannelId:   "sat-a",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "not-satellite", []*service.Service{{
		Id: "1234500101",
		Service: model.Service{
			Key: model.ServiceKey{NetworkID: 12345, StreamID: 0x2222, ServiceID: 101},
		},
		ChannelType: "BS",
		ChannelId:   "not-satellite",
	}}); err != nil {
		t.Fatal(err)
	}
	manager := service.NewManager(store, config.ChannelsConfig{})
	targets, err := NewGatherer(manager, testSources{}, 0).Targets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets = %#v, want one satellite common-data target", targets)
	}
	if !targets[0].CommonData || !targets[0].Probe || targets[0].ChannelType != "anything" || targets[0].Service.NetworkID != 4 {
		t.Fatalf("target = %#v, want ONID-based common-data target", targets[0])
	}
}

func TestTargetsUsesSDTTAnnouncementChannel(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(ctx, "sat", "target", []*service.Service{{
		Id: "0000400101",
		Service: model.Service{
			Key: model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101},
		},
		ChannelType: "sat",
		ChannelId:   "target",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "sat", "common", []*service.Service{{
		Id: "0000492900",
		Service: model.Service{
			Key: model.ServiceKey{NetworkID: 4, StreamID: 0x4031, ServiceID: 929},
		},
		ChannelType: "sat",
		ChannelId:   "common",
	}}); err != nil {
		t.Fatal(err)
	}
	manager := service.NewManager(store, config.ChannelsConfig{})
	if err := manager.UpsertCommonDataAnnouncement(ctx, model.CommonDataAnnouncement{
		Service: model.ServiceKey{NetworkID: 4, StreamID: 0x4031, ServiceID: 929}, DownloadID: 0x12345678, VersionID: 7,
	}, "sat", "target"); err != nil {
		t.Fatal(err)
	}
	targets, err := NewGatherer(manager, testSources{}, 0).Targets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets = %#v, want one common-data target", targets)
	}
	if !targets[0].CommonData || targets[0].Probe || targets[0].ChannelType != "sat" || targets[0].ChannelID != "common" {
		t.Fatalf("target = %#v, want SDTT common-data channel", targets[0])
	}
}

func TestTargetsRefreshesCommonDataWhenLogosArePresent(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	logoID, logoVersion, downloadID := int64(12), int64(3), int64(7)
	if err := store.ReplaceChannelServices(ctx, "sat", "service", []*service.Service{{
		Id: "0000400101",
		Service: model.Service{
			Key:  model.ServiceKey{NetworkID: 4, StreamID: 0x4010, ServiceID: 101},
			Logo: &model.LogoRef{LogoID: uint16(logoID), Version: new(uint16(logoVersion)), DownloadDataID: new(uint16(downloadID))},
		},
		ChannelType: "sat",
		ChannelId:   "service",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertLogo(ctx, 4, 0x4010, 101, logoID, 5, logoVersion, downloadID, []byte("png"), 1000); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "sat", "common", []*service.Service{{
		Id: "0000492900",
		Service: model.Service{
			Key: model.ServiceKey{NetworkID: 4, StreamID: 0x40f1, ServiceID: 929},
		},
		ChannelType: "sat",
		ChannelId:   "common",
	}}); err != nil {
		t.Fatal(err)
	}
	manager := service.NewManager(store, config.ChannelsConfig{})

	targets, err := NewGatherer(manager, testSources{}, 0).Targets(ctx)
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

func hasLogoTarget(targets []Target, channelType, channelID string, logoID int64, common bool) bool {
	for _, target := range targets {
		if target.ChannelType == channelType && target.ChannelID == channelID && int64(target.LogoID) == logoID && target.CommonData == common {
			return true
		}
	}
	return false
}
