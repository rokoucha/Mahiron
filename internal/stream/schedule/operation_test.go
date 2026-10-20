package schedule

import (
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/mmt/mmttest"
)

func TestOperationDoesNotInferAbsenceFromSilenceOrFutureParameters(t *testing.T) {
	for _, tc := range []struct {
		name       string
		parameters []byte
		want       bool
	}{
		{"current explicit absence", []byte{0, 0xEA, 0x60}, true},
		{"future absence", []byte{0, 0xFF, 0xFF}, false},
		{"malformed", []byte{0, 0xEA, 0x60, 0x58, 4, 0x40}, false},
		{"extended required", []byte{0, 0xEA, 0x60, 0x58, 4, 0x40, 0x08, 0x18, 0x00}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCollector(isdb.ScheduleTS)
			update := observe(t, c, section(0x50, 0, 0, 1, event(1, "one")))
			if update.ExtendedComplete {
				t.Fatal("silence treated as absence")
			}
			descriptors := []operationDescriptor{{0x41, []byte{0, 101, 1}}, {0xD7, tc.parameters}}
			now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
			// Use an unambiguously future descriptor relative to this date.
			if tc.name == "future absence" {
				day := uint16((now.Unix()+9*3600)/86400 + 40587 + 1)
				descriptors[1].data = []byte{0, byte(day >> 8), byte(day)}
			}
			calls := 0
			emit := func(u model.ScheduleUpdate) error { calls++; update = u; return nil }
			if err := c.observeOperation(4, descriptors, 0x41, 0xD7, 0x58, now, emit); err != nil {
				t.Fatal(err)
			}
			if update.ExtendedComplete != tc.want {
				t.Fatalf("complete=%v want %v", update.ExtendedComplete, tc.want)
			}
			first := calls
			_ = c.observeOperation(4, descriptors, 0x41, 0xD7, 0x58, now, emit)
			if calls != first {
				t.Fatal("repeated BIT resets stable timer")
			}
		})
	}
}

func TestMHBITMembershipAndOperation(t *testing.T) {
	c := NewCollector(isdb.ScheduleMMT)
	var update model.ScheduleUpdate
	_ = c.Observe(section(0x8C, 0, 0, 1, event(1, "one")), testNow, func(u model.ScheduleUpdate) error { update = u; return nil }, nil)
	descriptors := append(mmttest.Descriptor(0x800D, []byte{0, 101, 1}), mmttest.Descriptor(0x8017, []byte{0, 0xEA, 0x60})...)
	body := append(mmttest.Loop(nil), append([]byte{1}, mmttest.Loop(descriptors)...)...)
	data := mmt.Section(mmttest.Section(mmt.TableIDMHBIT, 4, 0, 0, 0, body))
	if err := c.ObserveMHBIT(data, testNow, func(u model.ScheduleUpdate) error { update = u; return nil }); err != nil {
		t.Fatal(err)
	}
	if !update.ExtendedComplete {
		t.Fatal("applicable MH-BIT absence ignored")
	}
	// A different broadcaster's service membership must not affect this service.
	c = NewCollector(isdb.ScheduleMMT)
	_ = c.Observe(section(0x8C, 0, 0, 1, event(1, "one")), testNow, func(u model.ScheduleUpdate) error { update = u; return nil }, nil)
	body = append(mmttest.Loop(nil), append([]byte{1}, mmttest.Loop(mmttest.Descriptor(0x800D, []byte{0, 102, 1}))...)...)
	_ = c.ObserveMHBIT(mmt.Section(mmttest.Section(mmt.TableIDMHBIT, 4, 0, 0, 0, body)), testNow, func(u model.ScheduleUpdate) error { update = u; return nil })
	if update.ExtendedComplete {
		t.Fatal("unrelated service established completion")
	}
}

func TestBITWithoutIndividualSIAllowsBasicOnlyCompletion(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	update := observe(t, c, section(0x50, 0, 0, 1, event(1, "one")))
	body := append(mmttest.Loop(nil), append([]byte{1}, mmttest.Loop([]byte{0x41, 3, 0, 101, 1})...)...)
	section := mmttest.Section(0xC4, 4, 0, 0, 0, body)
	if err := c.ObserveBIT(section, testNow, func(u model.ScheduleUpdate) error { update = u; return nil }); err != nil {
		t.Fatal(err)
	}
	if !update.ExtendedComplete {
		t.Fatal("broadcaster explicitly without individual SI was incomplete")
	}
}

func TestTerrestrialBITUsesActualSDTWithoutServiceList(t *testing.T) {
	for _, bitFirst := range []bool{true, false} {
		c := NewCollector(isdb.ScheduleTS)
		update := observe(t, c, section(0x50, 0, 0, 1, event(1, "one")))
		// TR-B14: the sole broadcaster loop has extended broadcaster + D7,
		// but no service-list descriptor. D7 advertises no extended schedule.
		descriptors := []byte{0xCE, 4, 0x10, 0, 1, 0, 0xD7, 3, 0xFF, 0xEA, 0x60}
		bit := mmttest.Section(0xC4, 4, 0, 0, 0, append(mmttest.Loop(nil), append([]byte{0}, mmttest.Loop(descriptors)...)...))
		service := append([]byte{0, 101, 3}, mmttest.Loop([]byte{0x48, 3, 1, 0, 0})...)
		sdt := mmttest.Section(0x42, testService.StreamID, 0, 0, 0, append([]byte{0, 4, 0xFF}, service...))
		emit := func(u model.ScheduleUpdate) error { update = u; return nil }
		if bitFirst {
			_ = c.ObserveBIT(bit, testNow, emit)
			if update.ExtendedComplete {
				t.Fatal("BIT without membership completed service")
			}
			_ = c.ObserveSDT(sdt, testNow, emit)
		} else {
			_ = c.ObserveSDT(sdt, testNow, emit)
			_ = c.ObserveBIT(bit, testNow, emit)
		}
		if !update.ExtendedComplete {
			t.Fatalf("terrestrial absence ignored, bitFirst=%v", bitFirst)
		}
	}
}

func TestMalformedBasicParametersDoNotEstablishExtendedAbsence(t *testing.T) {
	c := NewCollector(isdb.ScheduleTS)
	update := observe(t, c, section(0x50, 0, 0, 1, event(1, "one")))
	descriptors := []operationDescriptor{{0x41, []byte{0, 101, 1}}, {0xD7, []byte{0, 0xEA, 0x60, 0x50, 4, 0x40, 0xFF, 0x18, 0}}}
	_ = c.observeOperation(4, descriptors, 0x41, 0xD7, 0x58, testNow, func(u model.ScheduleUpdate) error { update = u; return nil })
	if update.ExtendedComplete {
		t.Fatal("malformed individual basic parameters established extended absence")
	}
}
