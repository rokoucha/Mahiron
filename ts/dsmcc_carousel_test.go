package ts

import (
	"errors"
	"testing"
)

func TestDSMCCCarouselReassemblesModule(t *testing.T) {
	module := []byte("hello carousel")
	carousel := NewDSMCCCarousel(DSMCCCarouselLimits{})
	dii, err := ParseDSMCCDII(buildGenericDSMCCDII(t, 0x12345678, 5, 0x2001, uint32(len(module)), 7, []byte("index.bml")))
	if err != nil {
		t.Fatal(err)
	}
	carousel.ObserveDII(dii)
	if _, ok := carousel.Module(0x2001); ok {
		t.Fatal("module completed before DDB blocks")
	}

	for blockNumber, off := uint16(0), 0; off < len(module); blockNumber, off = blockNumber+1, off+5 {
		end := min(off+5, len(module))
		ddb, err := ParseDSMCCDDB(buildDSMCCDDB(t, 0x12345678, 0x2001, 7, blockNumber, module[off:end]))
		if err != nil {
			t.Fatal(err)
		}
		completed, ok, err := carousel.ObserveDDB(ddb)
		if err != nil {
			t.Fatal(err)
		}
		if end < len(module) && ok {
			t.Fatal("module completed before all blocks")
		}
		if end == len(module) {
			if !ok {
				t.Fatal("module did not complete")
			}
			if string(completed.Data) != string(module) {
				t.Fatalf("module data = %q, want %q", completed.Data, module)
			}
		}
	}
}

func TestDSMCCCarouselIgnoresDuplicateBlocks(t *testing.T) {
	carousel := NewDSMCCCarousel(DSMCCCarouselLimits{})
	dii, err := ParseDSMCCDII(buildGenericDSMCCDII(t, 1, 2, 2, 4, 1, nil))
	if err != nil {
		t.Fatal(err)
	}
	carousel.ObserveDII(dii)
	ddb, err := ParseDSMCCDDB(buildDSMCCDDB(t, 1, 2, 1, 0, []byte("ab")))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := carousel.ObserveDDB(ddb); err != nil || ok {
		t.Fatalf("first block = %v, %v", ok, err)
	}
	if _, ok, err := carousel.ObserveDDB(ddb); err != nil || ok {
		t.Fatalf("duplicate block = %v, %v", ok, err)
	}
	ddb, err = ParseDSMCCDDB(buildDSMCCDDB(t, 1, 2, 1, 1, []byte("cd")))
	if err != nil {
		t.Fatal(err)
	}
	module, ok, err := carousel.ObserveDDB(ddb)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(module.Data) != "abcd" {
		t.Fatalf("module = %v, %q", ok, module.Data)
	}
}

func TestDSMCCCarouselResetsOnDIIChange(t *testing.T) {
	carousel := NewDSMCCCarousel(DSMCCCarouselLimits{})
	dii, err := ParseDSMCCDII(buildGenericDSMCCDII(t, 1, 2, 2, 4, 1, nil))
	if err != nil {
		t.Fatal(err)
	}
	carousel.ObserveDII(dii)
	ddb, err := ParseDSMCCDDB(buildDSMCCDDB(t, 1, 2, 1, 0, []byte("ab")))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := carousel.ObserveDDB(ddb); err != nil {
		t.Fatal(err)
	}

	dii, err = ParseDSMCCDII(buildGenericDSMCCDII(t, 1, 2, 2, 4, 2, nil))
	if err != nil {
		t.Fatal(err)
	}
	carousel.ObserveDII(dii)
	oldDDB, err := ParseDSMCCDDB(buildDSMCCDDB(t, 1, 2, 1, 1, []byte("cd")))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := carousel.ObserveDDB(oldDDB); err != nil || ok {
		t.Fatalf("old DDB completed after reset = %v, %v", ok, err)
	}
}

func TestDSMCCCarouselLimitsModuleSize(t *testing.T) {
	carousel := NewDSMCCCarousel(DSMCCCarouselLimits{MaxModuleSize: 3, MaxInFlightBytes: 10, MaxCompletedBytes: 10})
	dii, err := ParseDSMCCDII(buildGenericDSMCCDII(t, 1, 2, 2, 4, 1, nil))
	if err != nil {
		t.Fatal(err)
	}
	if accepted := carousel.ObserveDII(dii); len(accepted) != 0 {
		t.Fatalf("accepted modules = %d, want 0", len(accepted))
	}
	if got := carousel.InFlightBytes(); got != 0 {
		t.Fatalf("in-flight bytes = %d, want 0", got)
	}
}

func TestDSMCCCarouselEvictsForBudgets(t *testing.T) {
	carousel := NewDSMCCCarousel(DSMCCCarouselLimits{MaxModuleSize: 8, MaxInFlightBytes: 8, MaxCompletedBytes: 6})
	dii, err := ParseDSMCCDII(buildGenericDSMCCDII(t, 1, 4, 1, 8, 1, nil, dsmccModuleSpec{moduleID: 2, moduleSize: 4, version: 1}))
	if err != nil {
		t.Fatal(err)
	}
	carousel.ObserveDII(dii)
	if got := carousel.InFlightBytes(); got != 4 {
		t.Fatalf("in-flight bytes = %d, want oldest module evicted leaving 4", got)
	}

	ddb, err := ParseDSMCCDDB(buildDSMCCDDB(t, 1, 2, 1, 0, []byte("abcd")))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := carousel.ObserveDDB(ddb); err != nil || !ok {
		t.Fatalf("complete module 2 = %v, %v", ok, err)
	}

	dii, err = ParseDSMCCDII(buildGenericDSMCCDII(t, 1, 4, 2, 4, 1, nil, dsmccModuleSpec{moduleID: 3, moduleSize: 4, version: 1}))
	if err != nil {
		t.Fatal(err)
	}
	carousel.ObserveDII(dii)
	ddb, err = ParseDSMCCDDB(buildDSMCCDDB(t, 1, 3, 1, 0, []byte("efgh")))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := carousel.ObserveDDB(ddb); !errors.Is(err, ErrDSMCCCarouselBudgetExceeded) && (err != nil || !ok) {
		t.Fatalf("complete module 3 = %v, %v", ok, err)
	}
	if got := carousel.CompletedBytes(); got > 6 {
		t.Fatalf("completed bytes = %d, want <= 6", got)
	}
}

type dsmccModuleSpec struct {
	moduleID   uint16
	moduleSize uint32
	version    byte
}

func buildGenericDSMCCDII(t *testing.T, downloadID uint32, blockSize, moduleID uint16, moduleSize uint32, moduleVersion byte, moduleInfo []byte, extra ...dsmccModuleSpec) Section {
	t.Helper()
	body := []byte{
		byte(downloadID >> 24), byte(downloadID >> 16), byte(downloadID >> 8), byte(downloadID),
		byte(blockSize >> 8), byte(blockSize),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0,
	}
	count := 1 + len(extra)
	body = append(body, byte(count>>8), byte(count))
	body = appendDSMCCDIIModule(body, moduleID, moduleSize, moduleVersion, moduleInfo)
	for _, module := range extra {
		body = appendDSMCCDIIModule(body, module.moduleID, module.moduleSize, module.version, nil)
	}
	return buildDSMCCSection(t, TableIDDSMCCDII, 0x1002, 1, body)
}

func appendDSMCCDIIModule(body []byte, moduleID uint16, moduleSize uint32, moduleVersion byte, moduleInfo []byte) []byte {
	body = append(body,
		byte(moduleID>>8), byte(moduleID),
		byte(moduleSize>>24), byte(moduleSize>>16), byte(moduleSize>>8), byte(moduleSize),
		moduleVersion,
		byte(len(moduleInfo)),
	)
	return append(body, moduleInfo...)
}
