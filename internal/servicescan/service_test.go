package servicescan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/service"
)

func TestServiceScanChannelStoresScannedServicesAndReturnsNewNetworks(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &staticScanner{out: `[
		{"nid":4,"tsid":1,"sid":101,"name":"BS 101","type":1,"remoteControlKeyId":1},
		{"nid":4,"tsid":1,"sid":102,"name":"BS 102","type":1,"remoteControlKeyId":2},
		{"nid":5,"tsid":2,"sid":201,"name":"BS 201","type":2,"remoteControlKeyId":3}
	]`}

	got, err := NewService(manager, scanner, nil).ScanChannel(ctx, "BS", "BS01", true)
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
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: idFor(4, 101), NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := &staticScanner{out: `[
		{"nid":4,"tsid":1,"sid":101,"name":"known","type":1},
		{"nid":4,"tsid":1,"sid":102,"name":"new same network","type":1},
		{"nid":5,"tsid":1,"sid":201,"name":"new network","type":1},
		{"nid":5,"tsid":1,"sid":202,"name":"new network duplicate","type":1}
	]`}

	got, err := NewService(manager, scanner, nil).ScanChannel(ctx, "BS", "BS01", false)
	if err != nil {
		t.Fatal(err)
	}
	assertNIDs(t, got, map[uint16]bool{4: true, 5: true})
}

func TestServiceScanChannelReturnsNoNetworksWhenAllServicesKnown(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: idFor(4, 101), NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := &staticScanner{out: `[{"nid":4,"tsid":1,"sid":101,"name":"known","type":1}]`}

	got, err := NewService(manager, scanner, nil).ScanChannel(ctx, "BS", "BS01", false)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("new networks = %v, want nil", got)
	}
}

func TestServiceScanChannelReturnsInvalidJSON(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)

	_, err = NewService(manager, &staticScanner{out: `[`}, nil).ScanChannel(ctx, "BS", "BS01", false)
	if err == nil {
		t.Fatal("ScanChannel error = nil, want invalid JSON error")
	}
}

func TestServiceScanChannelReturnsScannerError(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	want := errors.New("scan failed")

	_, err = NewService(manager, &staticScanner{err: want}, nil).ScanChannel(ctx, "BS", "BS01", false)
	if !errors.Is(err, want) {
		t.Fatalf("ScanChannel error = %v, want %v", err, want)
	}
}

func TestServiceChannelsExcludesDisabledChannels(t *testing.T) {
	disabled := true
	channels := NewService(nil, nil, config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "28", IsDisabled: &disabled},
	}).Channels()

	if len(channels) != 1 || channels[0] != (Channel{Type: "GR", ID: "27"}) {
		t.Fatalf("channels = %#v, want only GR/27", channels)
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
	err  error
	out  string
	wait bool
}

func (s *staticScanner) ScanServices(_ context.Context, _ string, _ string, wait bool, dst io.Writer) error {
	s.wait = wait
	if s.err != nil {
		return s.err
	}
	_, err := io.Copy(dst, strings.NewReader(s.out))
	return err
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
