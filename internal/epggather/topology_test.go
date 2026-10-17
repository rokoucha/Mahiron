package epggather

import (
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
)

func TestNetworkFreshnessUsesOldestScheduledService(t *testing.T) {
	for _, tc := range []struct {
		name      string
		successes []int64
		want      int64
	}{
		{"all acquired", []int64{200, 100}, 100},
		{"unseen first", []int64{0, 100}, 0},
		{"unseen last", []int64{100, 0}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var services []*service.Service
			for i, success := range tc.successes {
				svc := &service.Service{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: uint16(i + 1)}, EITSchedule: true}, ChannelType: "BS", ChannelId: "BS01"}
				if success != 0 {
					svc.EPG.LastSuccessAt = &success
				}
				services = append(services, svc)
			}
			// Services without schedule EIT must not keep a network permanently due.
			services = append(services, &service.Service{Service: model.Service{Key: model.ServiceKey{NetworkID: 4, ServiceID: 9}}, ChannelType: "BS", ChannelId: "BS01"})
			groups := groupServicesByNetwork(services, config.ChannelsConfig{{Type: "BS", Channel: "BS01"}}, isdb.IsSatelliteOriginalNetworkID)
			if got := groups[4].OldestSuccessAt; got != tc.want {
				t.Fatalf("oldest success = %d, want %d", got, tc.want)
			}
		})
	}
}
