package epggather

import (
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
)

func sharedPeerEvents() (parent, child model.Event) {
	pattern := 0
	parent = model.Event{
		Key:         model.ServiceKey{NetworkID: 1, ServiceID: 101},
		EventID:     9,
		Name:        "parent title",
		Description: "parent description",
		Genres:      []model.Genre{{Lv1: 0, Lv2: 1, Un1: 15, Un2: 15}},
		Videos:      []model.VideoComponent{{Codec: model.VideoCodecMPEG2, Resolution: model.VideoResolution1080i}},
		Audios:      []model.AudioComponent{{ComponentType: 3}},
		Extended:    []model.ExtendedBlock{{Language: "jpn", Items: []model.ExtendedItem{{Name: "出演者", Text: "parent cast"}}}},
		Series:      &model.Series{ID: 7, Pattern: &pattern, Name: "series"},
	}
	child = model.Event{
		Key:     model.ServiceKey{NetworkID: 1, ServiceID: 102},
		EventID: 10,
		Related: []model.RelatedEvent{{GroupType: model.EventGroupShared, ServiceID: 101, EventID: 9}},
	}
	return parent, child
}

func TestFillEventsFromSharedPeersCopiesMissingDetails(t *testing.T) {
	parent, child := sharedPeerEvents()
	events := []model.Event{child, parent}

	fillEventsFromSharedPeers(events)

	got := events[0]
	if got.Name != parent.Name || got.Description != parent.Description {
		t.Fatalf("child text = %q/%q", got.Name, got.Description)
	}
	if len(got.Genres) != 1 || len(got.Videos) != 1 || len(got.Audios) != 1 || len(got.Extended) != 1 || got.Series == nil {
		t.Fatalf("child details were not filled: %#v", got)
	}
}

func TestFillEventsFromSharedPeersAcrossServices(t *testing.T) {
	parent, child := sharedPeerEvents()
	children, parents := []model.Event{child}, []model.Event{parent}

	fillEventsFromSharedPeers(children, parents)

	if children[0].Name != "parent title" {
		t.Fatalf("child name = %q, want the name shared from another service", children[0].Name)
	}
}

func TestFillEventsFromSharedPeersKeepsExistingDetails(t *testing.T) {
	parent, child := sharedPeerEvents()
	child.Name = "child title"
	events := []model.Event{child, parent}

	fillEventsFromSharedPeers(events)

	if events[0].Name != "child title" {
		t.Fatalf("child name = %q, want existing value", events[0].Name)
	}
}

func TestFillEventsFromSharedPeersUsesOneWaySharedGraph(t *testing.T) {
	source := model.Event{
		Key:     model.ServiceKey{NetworkID: 1, ServiceID: 102},
		EventID: 10,
		Related: []model.RelatedEvent{{GroupType: model.EventGroupShared, ServiceID: 101, EventID: 9}},
	}
	destination := model.Event{
		Key:     model.ServiceKey{NetworkID: 1, ServiceID: 101},
		EventID: 9,
		Name:    "destination title",
	}
	events := []model.Event{destination, source}

	fillEventsFromSharedPeers(events)

	if events[1].Name != "destination title" {
		t.Fatalf("source name = %q, want destination title", events[1].Name)
	}
}

func TestLowQualityEventWarning(t *testing.T) {
	events := make([]model.Event, 10)
	for i := range events {
		events[i].EventID = uint16(i + 1)
	}
	events[0].Name = "one title"
	if got := lowQualityEventWarning(events); got == "" {
		t.Fatal("warning = empty, want low quality warning")
	}
	events[1].Name = "second title"
	events[2].Name = "third title"
	if got := lowQualityEventWarning(events); got != "" {
		t.Fatalf("warning = %q, want empty", got)
	}
}
