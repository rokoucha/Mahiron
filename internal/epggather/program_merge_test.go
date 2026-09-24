package epggather

import (
	"testing"

	"github.com/21S1298001/mahiron/internal/program"
)

func TestFillProgramsFromSharedPeersCopiesMissingDetails(t *testing.T) {
	parent := &program.Program{
		ID:          program.ProgramID(1, 101, 9),
		NetworkID:   1,
		ServiceID:   101,
		EventID:     9,
		StartAt:     1000,
		Duration:    2000,
		Name:        "parent title",
		Description: "parent description",
		Genres:      []program.Genre{{Lv1: 0, Lv2: 1, Un1: 15, Un2: 15}},
		Video:       &program.Video{StreamContent: 1, ComponentType: 179},
		Audios:      []program.Audio{{ComponentType: 3}},
		Extended:    map[string]string{"出演者": "parent cast"},
		Series:      &program.Series{ID: 7, Name: "series"},
	}
	child := &program.Program{
		ID:        program.ProgramID(1, 102, 10),
		NetworkID: 1,
		ServiceID: 102,
		EventID:   10,
		StartAt:   1000,
		Duration:  2000,
		RelatedItems: []program.RelatedItem{
			{Type: program.RelatedItemTypeShared, ServiceID: 101, EventID: 9},
		},
	}

	fillProgramsFromSharedPeers([]*program.Program{child, parent})

	if child.Name != parent.Name || child.Description != parent.Description {
		t.Fatalf("child text = %q/%q", child.Name, child.Description)
	}
	if len(child.Genres) != 1 || child.Video == nil || len(child.Audios) != 1 || child.Extended["出演者"] != "parent cast" || child.Series == nil {
		t.Fatalf("child details were not filled: %#v", child)
	}
}

func TestFillProgramsFromSharedPeersKeepsExistingDetails(t *testing.T) {
	parent := &program.Program{
		ID:        program.ProgramID(1, 101, 9),
		NetworkID: 1,
		ServiceID: 101,
		EventID:   9,
		StartAt:   1000,
		Duration:  2000,
		Name:      "parent title",
	}
	child := &program.Program{
		ID:        program.ProgramID(1, 102, 10),
		NetworkID: 1,
		ServiceID: 102,
		EventID:   10,
		StartAt:   1000,
		Duration:  2000,
		Name:      "child title",
		RelatedItems: []program.RelatedItem{
			{Type: program.RelatedItemTypeShared, ServiceID: 101, EventID: 9},
		},
	}

	fillProgramsFromSharedPeers([]*program.Program{child, parent})

	if child.Name != "child title" {
		t.Fatalf("child name = %q, want existing value", child.Name)
	}
}

func TestFillProgramsFromSharedPeersUsesOneWaySharedGraph(t *testing.T) {
	source := &program.Program{
		ID:        program.ProgramID(1, 102, 10),
		NetworkID: 1,
		ServiceID: 102,
		EventID:   10,
		RelatedItems: []program.RelatedItem{
			{Type: program.RelatedItemTypeShared, ServiceID: 101, EventID: 9},
		},
	}
	destination := &program.Program{
		ID:        program.ProgramID(1, 101, 9),
		NetworkID: 1,
		ServiceID: 101,
		EventID:   9,
		Name:      "destination title",
	}

	fillProgramsFromSharedPeers([]*program.Program{destination, source})

	if source.Name != "destination title" {
		t.Fatalf("source name = %q, want destination title", source.Name)
	}
}

func TestLowQualityProgramWarning(t *testing.T) {
	var programs []*program.Program
	for i := 0; i < 10; i++ {
		programs = append(programs, &program.Program{ID: int64(i + 1)})
	}
	programs[0].Name = "one title"
	if got := lowQualityProgramWarning(programs); got == "" {
		t.Fatal("warning = empty, want low quality warning")
	}
	programs[1].Name = "second title"
	programs[2].Name = "third title"
	if got := lowQualityProgramWarning(programs); got != "" {
		t.Fatalf("warning = %q, want empty", got)
	}
}
