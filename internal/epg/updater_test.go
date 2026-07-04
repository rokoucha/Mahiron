package epg

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/program"
)

func newTestProgramManager(t *testing.T) *program.ProgramManager {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return program.NewProgramManager(program.NewSQLiteStore(database))
}

func TestUpsertEITSectionDecodesDescriptors(t *testing.T) {
	ctx := context.Background()
	manager := newTestProgramManager(t)
	updater := NewUpdater(manager)

	if err := updater.UpsertEITSection(ctx, sampleSection("気象情報・ニュース")); err != nil {
		t.Fatal(err)
	}

	p, ok, err := manager.Get(ctx, program.ProgramID(32736, 1024, 12250))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if p.Name != "気象情報・ニュース" {
		t.Fatalf("Name = %q", p.Name)
	}
	if p.Description != "説明" {
		t.Fatalf("Description = %q", p.Description)
	}
	if !p.IsFree {
		t.Fatal("IsFree = false")
	}
	if p.Video == nil || p.Video.StreamContent != 1 || p.Video.ComponentType != 179 {
		t.Fatalf("Video = %#v", p.Video)
	}
	if len(p.Audios) != 1 || p.Audios[0].SamplingRate == nil || *p.Audios[0].SamplingRate != 48000 {
		t.Fatalf("Audios = %#v", p.Audios)
	}
	if len(p.Genres) != 1 || p.Genres[0].Lv1 != 0 || p.Genres[0].Lv2 != 1 {
		t.Fatalf("Genres = %#v", p.Genres)
	}
}

func TestEITPFUpsertsExistingProgram(t *testing.T) {
	ctx := context.Background()
	manager := newTestProgramManager(t)
	updater := NewUpdater(manager)
	if err := updater.UpsertEITSection(ctx, sampleSection("気象情報・ニュース")); err != nil {
		t.Fatal(err)
	}

	if err := updater.UpsertEITSection(ctx, sampleSection("延長後ニュース")); err != nil {
		t.Fatal(err)
	}

	p, ok, err := manager.Get(ctx, program.ProgramID(32736, 1024, 12250))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if p.Name != "延長後ニュース" {
		t.Fatalf("Name = %q, want updated value", p.Name)
	}
}

func TestApplyDescriptorHandlesExtendedAndSeriesAndEventGroup(t *testing.T) {
	gt := 0x01
	prog := &program.Program{}
	applyDescriptor(prog, EITDescriptor{
		Type: "ExtendedEvent",
		Items: [][]string{
			{"出演者", "Foo"},
			{"概要", "Bar"},
		},
	})
	if prog.Extended["出演者"] != "Foo" || prog.Extended["概要"] != "Bar" {
		t.Fatalf("Extended = %#v", prog.Extended)
	}
	prog2 := &program.Program{}
	applyDescriptor(prog2, EITDescriptor{
		Type:       "Series",
		SeriesID:   ptrInt(11),
		SeriesName: "series-A",
	})
	if prog2.Series == nil || prog2.Series.ID != 11 || prog2.Series.Name != "series-A" {
		t.Fatalf("Series = %#v", prog2.Series)
	}
	prog3 := &program.Program{}
	applyDescriptor(prog3, EITDescriptor{
		Type:      "EventGroup",
		GroupType: &gt,
		Events:    []RelatedEvent{{ServiceID: 1, EventID: 2}},
	})
	if len(prog3.RelatedItems) != 1 || prog3.RelatedItems[0].Type != program.RelatedItemTypeShared {
		t.Fatalf("RelatedItems = %#v", prog3.RelatedItems)
	}
	movement := 0x03
	prog4 := &program.Program{}
	applyDescriptor(prog4, EITDescriptor{
		Type:      "EventGroup",
		GroupType: &movement,
		Events:    []RelatedEvent{{ServiceID: 1, EventID: 2}},
	})
	if len(prog4.RelatedItems) != 1 || prog4.RelatedItems[0].Type != program.RelatedItemTypeMovement {
		t.Fatalf("Movement RelatedItems = %#v", prog4.RelatedItems)
	}
}

func ptrInt(v int) *int { return &v }

func sampleSection(name string) *EITSection {
	return &EITSection{
		OriginalNetworkID: 32736,
		ServiceID:         1024,
		Events: []EITEvent{{
			EventID:   12250,
			StartTime: 1570917180000,
			Duration:  420000,
			Scrambled: false,
			Descriptors: []EITDescriptor{
				{Type: "ShortEvent", EventName: name, Text: "説明"},
				{Type: "Component", StreamContent: ptrInt(1), ComponentType: ptrInt(179)},
				{Type: "AudioComponent", ComponentType: ptrInt(1), ComponentTag: ptrInt(16), MainComponent: ptrBool(true), SamplingRate: ptrInt(7), Lang: "jpn"},
				{Type: "Content", Nibbles: [][]int{{0, 1, 15, 15}}},
			},
		}},
	}
}

func ptrBool(v bool) *bool { return &v }
