package logogather

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

type recordingStore struct {
	images []model.Logo
}

func (s *recordingStore) GetChannels() config.ChannelsConfig { return nil }

func (s *recordingStore) GetServices(context.Context) ([]*service.Service, error) { return nil, nil }

func (s *recordingStore) CommonDataAnnouncements(context.Context) ([]model.CommonDataAnnouncement, error) {
	return nil, nil
}

func (s *recordingStore) UpsertLogoImage(_ context.Context, image model.Logo) error {
	s.images = append(s.images, image)
	return nil
}

func TestGatherChannelCompletesWhenTargetsArrive(t *testing.T) {
	logo := model.Logo{NetworkID: 0x000B, LogoID: 101, Version: 1, DownloadDataID: 1, LogoType: 7, Data: []byte("png")}
	store := &recordingStore{}
	calls := 0
	gatherer := NewGatherer(store, testSources{logos: []model.Logo{logo}, calls: &calls}, time.Minute)
	target := Target{Service: model.ServiceKey{NetworkID: 0x000B, StreamID: 0xB110, ServiceID: 101}, ChannelType: "BS4K", ChannelID: "BS1_0", LogoID: 101, Version: 1, DownloadDataID: 1}

	started := time.Now()
	if err := gatherer.GatherChannel(context.Background(), "BS4K", "BS1_0", []Target{target}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("gathering took %s, want it to stop once the target arrived", elapsed)
	}
	if calls != 1 || len(store.images) != 1 || !reflect.DeepEqual(store.images[0], logo) {
		t.Fatalf("calls = %d, stored = %+v, want the observed logo stored once", calls, store.images)
	}
}

func TestGatherChannelTimeoutIsSuccessful(t *testing.T) {
	gatherer := NewGatherer(&recordingStore{}, testSources{}, time.Millisecond)
	target := Target{Service: model.ServiceKey{NetworkID: 4, ServiceID: 101}, ChannelType: "BS", ChannelID: "BS01", LogoID: 12, Version: 3, DownloadDataID: 7}
	if err := gatherer.GatherChannel(context.Background(), "BS", "BS01", []Target{target}); err != nil {
		t.Fatalf("timed out gathering failed: %v", err)
	}
}

func TestRemoteImagesCompleteOnlyTheirNamedServices(t *testing.T) {
	key := model.ServiceKey{NetworkID: 32736, StreamID: 32736, ServiceID: 1024}
	other := key
	other.ServiceID++
	store := &recordingStore{}
	logos := []model.Logo{
		{NetworkID: key.NetworkID, LogoID: 0, Unversioned: true, Services: []model.ServiceKey{key}, Data: []byte("first")},
		{NetworkID: key.NetworkID, LogoID: 0, Unversioned: true, Services: []model.ServiceKey{other}, Data: []byte("second")},
	}
	g := NewGatherer(store, testSources{logos: logos}, time.Second)
	targets := []Target{{Service: key, LogoID: 0, Version: 2, DownloadDataID: 3}, {Service: other, LogoID: 0, Version: 2, DownloadDataID: 3}}
	if err := g.GatherChannel(t.Context(), "GR", "27", targets); err != nil {
		t.Fatal(err)
	}
	if len(store.images) != 2 {
		t.Fatalf("stored %d images, want both services", len(store.images))
	}
}

func TestUnresolvedTargetDoesNotBlockBroadcastCompletion(t *testing.T) {
	logo := model.Logo{NetworkID: 32736, LogoID: 0, Version: 2, DownloadDataID: 1, Data: []byte("png")}
	store := &recordingStore{}
	g := NewGatherer(store, testSources{logos: []model.Logo{logo}}, time.Second)
	targets := []Target{
		{Service: model.ServiceKey{NetworkID: 32736, ServiceID: 1024}, LogoID: 0, Version: 2, DownloadDataID: 1},
		{Service: model.ServiceKey{NetworkID: 32736, ServiceID: 1025}, LogoID: 0, Unresolved: true},
	}
	started := time.Now()
	if err := g.GatherChannel(t.Context(), "GR", "27", targets); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second/2 {
		t.Fatal("unresolved target prevented early completion")
	}
}

func TestNamedBroadcastImagesCompleteOnlyTheirServices(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 101}
	other := key
	other.ServiceID++
	store := &recordingStore{}
	images := []model.Logo{
		{NetworkID: 4, LogoID: 12, Version: 2, DownloadDataID: 3, Services: []model.ServiceKey{key}, Data: []byte("one")},
		{NetworkID: 4, LogoID: 12, Version: 2, DownloadDataID: 3, Services: []model.ServiceKey{other}, Data: []byte("two")},
	}
	g := NewGatherer(store, testSources{logos: images}, time.Second)
	targets := []Target{{Service: key, LogoID: 12, Version: 2, DownloadDataID: 3}, {Service: other, LogoID: 12, Version: 2, DownloadDataID: 3}}
	if err := g.GatherChannel(t.Context(), "BS", "A", targets); err != nil {
		t.Fatal(err)
	}
	if len(store.images) != 2 {
		t.Fatal("one service's image completed the other service")
	}
}
