package ts

import "testing"

func TestDSMCCAnnouncementsExposeBlockProgress(t *testing.T) {
	carousel := NewDSMCCCarousel(DSMCCCarouselLimits{})
	carousel.ObserveDII(&DSMCCDII{DownloadID: 1, BlockSize: 2, Modules: []DSMCCModuleInfo{{ModuleID: 1, ModuleSize: 3, Version: 1}}})
	announcement := carousel.Announcements()[0]
	if announcement.ReceivedBlocks != 0 || announcement.TotalBlocks != 2 {
		t.Fatalf("initial progress = %#v", announcement)
	}
	_, _, _, _ = carousel.ObserveDDBWithResult(&DSMCCDDB{DownloadID: 1, ModuleID: 1, ModuleVersion: 1, BlockNumber: 0, Data: []byte{1, 2}})
	announcement = carousel.Announcements()[0]
	if announcement.ReceivedBlocks != 1 || announcement.TotalBlocks != 2 {
		t.Fatalf("receiving progress = %#v", announcement)
	}
}

func TestDSMCCRejectedAnnouncementsExposeReason(t *testing.T) {
	carousel := NewDSMCCCarousel(DSMCCCarouselLimits{MaxModuleSize: 2})
	carousel.ObserveDII(&DSMCCDII{DownloadID: 1, BlockSize: 1, Modules: []DSMCCModuleInfo{{ModuleID: 7, ModuleSize: 3, Version: 1}}})
	rejected := carousel.RejectedAnnouncements()
	if len(rejected) != 1 || rejected[0].Module.ModuleID != 7 || rejected[0].Reason != "moduleSizeLimitExceeded" {
		t.Fatalf("rejected = %#v", rejected)
	}
	if len(carousel.Announcements()) != 0 {
		t.Fatal("rejected module was exposed as accepted")
	}
}
