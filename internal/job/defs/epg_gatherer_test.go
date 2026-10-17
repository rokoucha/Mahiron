package defs

import (
	"context"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/epggather"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/google/go-cmp/cmp"
)

type epgQueue struct{ definitions []job.JobDefinition }

func (*epgQueue) Register(job.JobDefinition) {}
func (q *epgQueue) EnqueueDefinition(def job.JobDefinition) (string, error) {
	q.definitions = append(q.definitions, def)
	return def.Key, nil
}

type scheduledEPG struct {
	groups   map[uint16]*epggather.Network
	inputs   []model.ServiceKey
	gathered []model.ServiceKey
	gather   func(context.Context, uint16) error
}

func (s *scheduledEPG) Groups(context.Context) (map[uint16]*epggather.Network, error) {
	return s.groups, nil
}
func (s *scheduledEPG) BuildNetworkInputs(context.Context, uint16) ([]epggather.Candidate, []model.ServiceKey, error) {
	return nil, s.inputs, nil
}
func (s *scheduledEPG) GatherNetwork(ctx context.Context, nid uint16, _ []epggather.Candidate, keys []model.ServiceKey) error {
	if s.gather != nil {
		return s.gather(ctx, nid)
	}
	s.gathered = keys
	return nil
}

func TestEPGNetworksGatherConcurrently(t *testing.T) {
	mgr, err := job.NewManager(job.Config{MaxConcurrentJobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mgr.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	started := make(chan uint16, 2)
	service := &scheduledEPG{
		groups: map[uint16]*epggather.Network{
			1: {Services: []model.ServiceKey{{NetworkID: 1, ServiceID: 1}}},
			2: {Services: []model.ServiceKey{{NetworkID: 2, ServiceID: 1}}},
		},
		inputs: []model.ServiceKey{{ServiceID: 1}},
		gather: func(ctx context.Context, nid uint16) error {
			started <- nid
			<-ctx.Done()
			return ctx.Err()
		},
	}
	if _, err := mgr.EnqueueDefinition(job.JobDefinition{
		Key: EPGGathererKey, ExclusiveKeys: []string{"epg-service-topology"},
		Handler: epgGathererHandler(mgr, service, nil),
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("EPG jobs did not start concurrently")
		}
	}
	if _, err := mgr.EnqueueDefinition(job.JobDefinition{
		Key: "scan", ExclusiveKeys: []string{"epg-service-topology"},
		Handler: func(context.Context) error { t.Error("scan overlapped EPG gathering"); return nil },
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEPGDispatchPrioritizesUnseenAndOldNetworks(t *testing.T) {
	now := time.Now()
	service := &scheduledEPG{groups: map[uint16]*epggather.Network{}}
	for nid, age := range map[uint16]time.Duration{1: time.Hour, 2: 0, 3: 2 * time.Hour, 4: time.Minute, 5: time.Hour} {
		group := &epggather.Network{Services: []model.ServiceKey{{NetworkID: nid, ServiceID: 1}}}
		if age != 0 {
			group.OldestSuccessAt = now.Add(-age).UnixMilli()
		}
		service.groups[nid] = group
	}
	queue := &epgQueue{}
	if err := epgGathererHandler(queue, service, nil)(t.Context()); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, def := range queue.definitions {
		keys = append(keys, def.Key)
	}
	if diff := cmp.Diff([]string{"epg-gather:nid:2", "epg-gather:nid:3", "epg-gather:nid:1", "epg-gather:nid:5"}, keys); diff != "" {
		t.Fatal(diff)
	}
	for i := 1; i < len(queue.definitions); i++ {
		if queue.definitions[i].Priority > queue.definitions[i-1].Priority {
			t.Fatal("queue priority disagrees with dispatch order")
		}
	}
}

func TestEPGQueuedJobReloadsTopology(t *testing.T) {
	old := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	replacement := model.ServiceKey{NetworkID: 4, ServiceID: 102}
	service := &scheduledEPG{inputs: []model.ServiceKey{replacement}}
	queue := &epgQueue{}
	if _, err := enqueueEPGGatherForNetwork(t.Context(), queue, service, 4, &epggather.Network{Services: []model.ServiceKey{old}}); err != nil {
		t.Fatal(err)
	}
	if err := queue.definitions[0].Handler(t.Context()); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]model.ServiceKey{replacement}, service.gathered); diff != "" {
		t.Fatal(diff)
	}
	service.inputs = nil
	service.gathered = nil
	if err := queue.definitions[0].Handler(t.Context()); err != nil {
		t.Fatal(err)
	}
	if service.gathered != nil {
		t.Fatal("gathered a removed network")
	}
}
