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
	groups map[epggather.GroupKey]*epggather.Group
	gather func(context.Context, epggather.GroupKey) error
}

func (s *scheduledEPG) Groups(context.Context) (map[epggather.GroupKey]*epggather.Group, error) {
	return s.groups, nil
}
func (s *scheduledEPG) GatherGroup(ctx context.Context, key epggather.GroupKey) error {
	if s.gather != nil {
		return s.gather(ctx, key)
	}
	return nil
}

func TestEPGStreamsInSameNetworkGatherConcurrently(t *testing.T) {
	mgr, err := job.NewManager(job.Config{MaxConcurrentJobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mgr.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	started := make(chan epggather.GroupKey, 2)
	service := &scheduledEPG{
		groups: map[epggather.GroupKey]*epggather.Group{
			{NetworkID: 11, StreamID: 100}: {Services: []model.ServiceKey{{NetworkID: 11, StreamID: 100, ServiceID: 1}}},
			{NetworkID: 11, StreamID: 101}: {Services: []model.ServiceKey{{NetworkID: 11, StreamID: 101, ServiceID: 2}}},
		},
		gather: func(ctx context.Context, key epggather.GroupKey) error {
			started <- key
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
	service := &scheduledEPG{groups: map[epggather.GroupKey]*epggather.Group{}}
	for nid, age := range map[uint16]time.Duration{1: time.Hour, 2: 0, 3: 2 * time.Hour, 4: time.Minute, 5: time.Hour} {
		group := &epggather.Group{Services: []model.ServiceKey{{NetworkID: nid, ServiceID: 1}}}
		if age != 0 {
			group.OldestSuccessAt = now.Add(-age).UnixMilli()
		}
		service.groups[epggather.GroupKey{NetworkID: nid, Stored: true}] = group
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

func TestEPGStoredAndStreamJobsHaveDistinctKeys(t *testing.T) {
	queue := &epgQueue{}
	service := &scheduledEPG{}
	group := &epggather.Group{Services: []model.ServiceKey{{NetworkID: 4, ServiceID: 101}}}
	for _, key := range []epggather.GroupKey{{NetworkID: 4, Stored: true}, {NetworkID: 4, StreamID: 0}, {NetworkID: 4, StreamID: 100}} {
		if _, err := enqueueEPGGatherGroup(queue, service, key, group); err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	for _, def := range queue.definitions {
		keys = append(keys, def.Key)
	}
	if diff := cmp.Diff([]string{"epg-gather:nid:4", "epg-gather:nid:4:stream:0", "epg-gather:nid:4:stream:100"}, keys); diff != "" {
		t.Fatal(diff)
	}
}

func TestUnknownStreamsHaveDistinctJobKeys(t *testing.T) {
	queue := &epgQueue{}
	service := &scheduledEPG{}
	group := &epggather.Group{Services: []model.ServiceKey{{NetworkID: 4, ServiceID: 151}}}
	for _, ch := range []string{"A", "B"} {
		key := epggather.GroupKey{NetworkID: 4, Channel: epggather.Candidate{Type: "CUSTOM", Channel: ch}}
		if _, err := enqueueEPGGatherGroup(queue, service, key, group); err != nil {
			t.Fatal(err)
		}
	}
	if queue.definitions[0].Key == queue.definitions[1].Key {
		t.Fatal("unknown stream jobs collide")
	}
}
