package logoref

import (
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
)

func TestResolve(t *testing.T) {
	services := map[uint16]*model.Service{
		1: {Logo: &model.LogoRef{LogoID: 5}},
		2: {Logo: &model.LogoRef{LogoID: 6}},
		3: {},
		4: nil,
		6: {Logo: &model.LogoRef{LogoID: 7}},
	}
	Resolve(services, map[uint16]Direct{5: {Version: 2, DownloadDataID: 9}, 7: {Version: 3, DownloadDataID: 10}}, map[uint16]uint16{1: 5, 2: 6, 3: 5, 4: 5, 5: 5, 6: 7})
	for _, id := range []uint16{1, 6} {
		logo := services[id].Logo
		want := uint16(2)
		if id == 6 {
			want = 3
		}
		if logo.Version == nil || *logo.Version != want || logo.DownloadDataID == nil || *logo.DownloadDataID != want+7 {
			t.Fatalf("service %d logo = %+v", id, logo)
		}
	}
	if services[2].Logo.Version != nil || services[2].Logo.DownloadDataID != nil || services[3].Logo != nil {
		t.Fatal("unresolved or absent logo was changed")
	}
}
