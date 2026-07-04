package servicescan

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/ts"
)

func TestServiceScanChannelStoresScannedServicesAndReturnsNewNetworks(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 101, Name: "BS 101", Type: 1, EITScheduleFlag: true, EITPresentFollowing: true, RemoteControlKeyId: uint8Ptr(1)},
		{Nid: 4, Tsid: 1, Sid: 102, Name: "BS 102", Type: 1, EITScheduleFlag: false, EITPresentFollowing: true, RemoteControlKeyId: uint8Ptr(2)},
		{Nid: 5, Tsid: 2, Sid: 201, Name: "BS 201", Type: 2, EITScheduleFlag: true, EITPresentFollowing: false, RemoteControlKeyId: uint8Ptr(3)},
	}}

	got, err := NewService(manager, scanner, nil, time.Second).ScanChannel(ctx, "BS", "BS01", true)
	if err != nil {
		t.Fatal(err)
	}
	assertNIDs(t, got, map[uint16]bool{4: true, 5: true})

	services, err := store.GetByChannel(ctx, "BS", "BS01")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(services), 3; got != want {
		t.Fatalf("stored services = %d, want %d", got, want)
	}
	if got, want := services[0].Id, "0000400101"; got != want {
		t.Fatalf("service id = %q, want %q", got, want)
	}
	if got, want := services[0].RemoteControlKeyId, uint8(1); got != want {
		t.Fatalf("remoteControlKeyId = %d, want %d", got, want)
	}
	if !services[0].EITScheduleFlag || !services[0].EITPresentFollowing {
		t.Fatalf("service 101 EIT flags = %v/%v, want true/true", services[0].EITScheduleFlag, services[0].EITPresentFollowing)
	}
	if services[1].EITScheduleFlag || !services[1].EITPresentFollowing {
		t.Fatalf("service 102 EIT flags = %v/%v, want false/true", services[1].EITScheduleFlag, services[1].EITPresentFollowing)
	}
	if !scanner.wait {
		t.Fatal("scanner wait = false, want true")
	}
}

func TestServiceScanChannelReturnsOnlyNewNetworks(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: idFor(4, 101), NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 101, Name: "known", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 102, Name: "new same network", Type: 1},
		{Nid: 5, Tsid: 1, Sid: 201, Name: "new network", Type: 1},
		{Nid: 5, Tsid: 1, Sid: 202, Name: "new network duplicate", Type: 1},
	}}

	got, err := NewService(manager, scanner, nil, time.Second).ScanChannel(ctx, "BS", "BS01", false)
	if err != nil {
		t.Fatal(err)
	}
	assertNIDs(t, got, map[uint16]bool{4: true, 5: true})
}

func TestServiceScanReportsNamedServiceResults(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: idFor(4, 100), NetworkId: 4, ServiceId: 100, TransportStreamId: 1, Name: "Removed", ChannelType: "BS", ChannelId: "BS01"},
		{Id: idFor(4, 101), NetworkId: 4, ServiceId: 101, TransportStreamId: 1, Name: "Known", ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	reporter := &captureReporter{}
	ctx = run.WithReporter(ctx, reporter)
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 101, Name: "Known", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 102, Name: "New Service", Type: 1, RemoteControlKeyId: uint8Ptr(2)},
	}}

	if _, err := NewService(manager, scanner, nil, time.Second).ScanChannel(ctx, "BS", "BS01", false); err != nil {
		t.Fatal(err)
	}
	if reporter.result == nil {
		t.Fatal("expected service scan result")
	}
	if reporter.result.Counts["addedServices"] != 1 || reporter.result.Counts["removedServices"] != 1 {
		t.Fatalf("result counts = %#v", reporter.result.Counts)
	}
	changes := map[string]string{}
	for _, item := range reporter.result.Items {
		if item.Kind != "service" {
			continue
		}
		name, _ := item.Data["name"].(string)
		change, _ := item.Data["change"].(string)
		changes[name] = change
	}
	if changes["New Service"] != "added" || changes["Removed"] != "removed" || changes["Known"] != "unchanged" {
		t.Fatalf("service result changes = %#v", changes)
	}
}

func TestServiceScanChannelReturnsNoNetworksWhenAllServicesKnown(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: idFor(4, 101), NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := &staticScanner{services: []ts.ServiceInfo{{Nid: 4, Tsid: 1, Sid: 101, Name: "known", Type: 1}}}

	got, err := NewService(manager, scanner, nil, time.Second).ScanChannel(ctx, "BS", "BS01", false)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("new networks = %v, want nil", got)
	}
}

func TestServiceScanChannelReturnsScannerError(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	want := errors.New("scan failed")

	_, err = NewService(manager, &staticScanner{err: want}, nil, time.Second).ScanChannel(ctx, "BS", "BS01", false)
	if !errors.Is(err, want) {
		t.Fatalf("ScanChannel error = %v, want %v", err, want)
	}
}

func TestServiceScanChannelTimesOutAndPreservesStoredServices(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	want := &service.Service{
		Id: idFor(4, 101), NetworkId: 4, ServiceId: 101,
		ChannelType: "BS", ChannelId: "BS01", Name: "stored",
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{want}); err != nil {
		t.Fatal(err)
	}

	_, err = NewService(manager, blockingScanner{}, nil, 10*time.Millisecond).ScanChannel(ctx, "BS", "BS01", true)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ScanChannel error = %v, want context deadline exceeded", err)
	}
	got, err := store.GetByChannel(ctx, "BS", "BS01")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Id != want.Id || got[0].Name != want.Name {
		t.Fatalf("stored services after timeout = %#v, want preserved %#v", got, want)
	}
}

func TestServiceScanTimeoutDoesNotApplyToAcquireContext(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &contextCapturingScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 101, Name: "BS 101", Type: 1},
	}}

	if _, err := NewService(manager, scanner, nil, time.Minute).ScanChannel(ctx, "BS", "BS01", true); err != nil {
		t.Fatal(err)
	}
	if _, ok := scanner.acquireCtx.Deadline(); ok {
		t.Fatal("acquire context should not inherit service scan timeout")
	}
	deadline, ok := scanner.scanCtx.Deadline()
	if !ok {
		t.Fatal("scan context should have service scan timeout")
	}
	if time.Until(deadline) <= 0 {
		t.Fatal("scan context deadline is already expired")
	}
	if !scanner.wait {
		t.Fatal("scanner wait = false, want true")
	}
}

func TestServiceChannelsExcludesDisabledChannels(t *testing.T) {
	disabled := true
	channels := NewService(nil, nil, config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "28", IsDisabled: &disabled},
	}, time.Second).Channels()

	if len(channels) != 1 || channels[0] != (Channel{Type: "GR", ID: "27"}) {
		t.Fatalf("channels = %#v, want only GR/27", channels)
	}
}

func TestServiceChannelsDedupesSameTypeAndChannelWithDifferentServiceIds(t *testing.T) {
	channels := NewService(nil, nil, config.ChannelsConfig{
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(100)},
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(119)},
	}, time.Second).Channels()

	if len(channels) != 1 || channels[0] != (Channel{Type: "EXT1", ID: "38"}) {
		t.Fatalf("channels = %#v, want single deduped EXT1/38", channels)
	}
}

func TestServiceScanChannelFiltersToConfiguredServiceId(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 100, Name: "other service", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 119, Name: "iTSCOMLive", Type: 1},
	}}
	channels := config.ChannelsConfig{{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(119)}}

	if _, err := NewService(manager, scanner, channels, time.Second).ScanChannel(ctx, "EXT1", "38", false); err != nil {
		t.Fatal(err)
	}
	services, err := store.GetByChannel(ctx, "EXT1", "38")
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 1 || services[0].ServiceId != 119 {
		t.Fatalf("stored services = %#v, want only sid 119", services)
	}
}

func TestServiceScanChannelUnionsServiceIdsAcrossMultipleEnabledEntries(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 100, Name: "first", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 119, Name: "second", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 200, Name: "excluded", Type: 1},
	}}
	channels := config.ChannelsConfig{
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(100)},
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(119)},
	}

	if _, err := NewService(manager, scanner, channels, time.Second).ScanChannel(ctx, "EXT1", "38", false); err != nil {
		t.Fatal(err)
	}
	services, err := store.GetByChannel(ctx, "EXT1", "38")
	if err != nil {
		t.Fatal(err)
	}
	got := map[uint16]bool{}
	for _, svc := range services {
		got[svc.ServiceId] = true
	}
	if len(got) != 2 || !got[100] || !got[119] {
		t.Fatalf("stored services = %#v, want sids 100 and 119", services)
	}
}

func TestServiceScanChannelDoesNotFilterWhenAnyEnabledEntryLacksServiceId(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 100, Name: "first", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 119, Name: "second", Type: 1},
	}}
	channels := config.ChannelsConfig{{Type: "EXT1", Channel: "38"}}

	if _, err := NewService(manager, scanner, channels, time.Second).ScanChannel(ctx, "EXT1", "38", false); err != nil {
		t.Fatal(err)
	}
	services, err := store.GetByChannel(ctx, "EXT1", "38")
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 2 {
		t.Fatalf("stored services = %#v, want all 2 unfiltered", services)
	}
}

func TestServiceScanChannelIgnoresDisabledEntryServiceId(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 100, Name: "disabled-only", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 119, Name: "enabled", Type: 1},
	}}
	disabled := true
	channels := config.ChannelsConfig{
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(100), IsDisabled: &disabled},
		{Type: "EXT1", Channel: "38", ServiceId: uint32Ptr(119)},
	}

	if _, err := NewService(manager, scanner, channels, time.Second).ScanChannel(ctx, "EXT1", "38", false); err != nil {
		t.Fatal(err)
	}
	services, err := store.GetByChannel(ctx, "EXT1", "38")
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 1 || services[0].ServiceId != 119 {
		t.Fatalf("stored services = %#v, want only sid 119 (disabled entry's sid 100 excluded)", services)
	}
}

func TestNewNetworkIDsFromDiffEmptyInputs(t *testing.T) {
	if got := newNetworkIDsFromDiff(nil, nil); got != nil {
		t.Errorf("nil scanned = %v, want nil", got)
	}
	before := map[string]struct{}{idFor(1, 101): {}}
	allKnown := []*service.Service{
		{Id: idFor(1, 101), NetworkId: 1, ServiceId: 101},
	}
	if got := newNetworkIDsFromDiff(before, allKnown); got != nil {
		t.Errorf("all-known scanned = %v, want nil", got)
	}
}

type staticScanner struct {
	err      error
	services []ts.ServiceInfo
	wait     bool
}

type blockingScanner struct{}

type contextCapturingScanner struct {
	acquireCtx context.Context
	scanCtx    context.Context
	services   []ts.ServiceInfo
	wait       bool
}

type captureReporter struct {
	result *run.Result
}

func (r *captureReporter) SetJobResult(result run.Result) {
	r.result = run.Clone(&result)
}

func (blockingScanner) ScanServices(ctx context.Context, _, _ string, _ bool) ([]ts.ServiceInfo, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *staticScanner) ScanServices(_ context.Context, _ string, _ string, wait bool) ([]ts.ServiceInfo, error) {
	s.wait = wait
	if s.err != nil {
		return nil, s.err
	}
	return s.services, nil
}

func (s *contextCapturingScanner) ScanServices(context.Context, string, string, bool) ([]ts.ServiceInfo, error) {
	panic("ScanServices should not be called when ScanServicesWithAcquireContext is implemented")
}

func (s *contextCapturingScanner) ScanServicesWithAcquireContext(scanCtx, acquireCtx context.Context, _ string, _ string, wait bool) ([]ts.ServiceInfo, error) {
	s.scanCtx = scanCtx
	s.acquireCtx = acquireCtx
	s.wait = wait
	return s.services, nil
}

func assertNIDs(t *testing.T, got []uint16, want map[uint16]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("new networks = %v, want %v", got, want)
	}
	for _, nid := range got {
		if !want[nid] {
			t.Errorf("unexpected NID %d in result %v", nid, got)
		}
	}
}

func idFor(nid, sid uint16) string {
	return fmt.Sprintf("%05d%05d", nid, sid)
}

func uint8Ptr(v uint8) *uint8 { return &v }

func uint32Ptr(v uint32) *uint32 { return &v }
