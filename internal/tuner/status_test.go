package tuner

import (
	"context"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
)

func TestTunerStatusTracksChannelsProcessAndUsers(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "test", Types: []string{"CATV"}, Command: "sleep 10"},
	}})
	requested := &config.ChannelConfig{Name: "Logical", Type: "BS", Channel: "101"}
	tuned := &config.ChannelConfig{Name: "Logical", Type: "CATV", Channel: "C13"}
	device, _, err := mgr.AcquireDevice(context.Background(), "CATV", requested, tuned, false)
	if err != nil {
		t.Fatal(err)
	}
	tracked := device.(interface {
		AddUser(User)
		RemoveUser(string)
		UpdateUserStreamInfo(string, string, StreamInfo)
	})
	tracked.AddUser(User{ID: "viewer", Priority: 1, Agent: "test"})
	tracked.UpdateUserStreamInfo("viewer", "BS/101", StreamInfo{Packet: 12, Drop: 1})
	if err := device.Start(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}

	status, ok := mgr.Status(0)
	if !ok {
		t.Fatal("status not found")
	}
	if status.CurrentChannelType != "BS" || status.CurrentChannel != "101" {
		t.Fatalf("current channel = %s/%s", status.CurrentChannelType, status.CurrentChannel)
	}
	if status.TunedChannelType != "CATV" || status.TunedChannel != "C13" {
		t.Fatalf("tuned channel = %s/%s", status.TunedChannelType, status.TunedChannel)
	}
	if status.PID <= 0 || status.Command != "sleep 10" {
		t.Fatalf("pid = %d, command = %q", status.PID, status.Command)
	}
	if !status.IsAvailable || !status.IsUsing || status.IsFree || len(status.Users) != 1 {
		t.Fatalf("unexpected active status: %+v", status)
	}
	if info := status.Users[0].StreamInfo["BS/101"]; info.Packet != 12 || info.Drop != 1 {
		t.Fatalf("stream info = %+v", status.Users[0].StreamInfo)
	}

	tracked.RemoveUser("viewer")
	if err := device.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, _ = mgr.Status(0)
	if !status.IsAvailable || !status.IsFree || status.IsUsing || status.PID != 0 || len(status.Users) != 0 {
		t.Fatalf("unexpected released status: %+v", status)
	}
	if status.CurrentChannel != "" || status.TunedChannel != "" {
		t.Fatalf("released channels were not cleared: %+v", status)
	}
}

func TestTunerStatusMarksUnexpectedProcessExitAsFault(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "broken", Types: []string{"GR"}, Command: "command-that-does-not-exist"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := device.Start(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner process did not exit")
	}
	deadline := time.Now().Add(time.Second)
	for {
		status, _ := mgr.Status(0)
		if status.IsFault {
			if status.IsAvailable {
				t.Fatalf("faulted tuner is available: %+v", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tuner was not marked faulted: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
	_ = device.Stop(context.Background())
}

func TestDisabledAndDVBTunerStatus(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "disabled", Types: []string{"GR"}, Command: "sleep 1", IsDisabled: true},
		{Name: "dvb", Types: []string{"SKY"}, Command: "sleep 1", DvbDevicePath: "/dev/null"},
	}})
	statuses := mgr.Statuses()
	if statuses[0].IsAvailable || statuses[0].IsFree {
		t.Fatalf("disabled tuner is available: %+v", statuses[0])
	}
	if !statuses[1].IsAvailable || !statuses[1].IsFree {
		t.Fatalf("dvb tuner is not available: %+v", statuses[1])
	}
	if _, ok := mgr.Status(2); ok {
		t.Fatal("out-of-range tuner status found")
	}
}

func TestTunerStatusSortsTypes(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "test", Types: []string{"SKY", "GR", "BS", "GR"}, Command: "sleep 1"},
	}})
	statuses := mgr.Statuses()
	want := []string{"BS", "GR", "SKY"}
	if !slices.Equal(statuses[0].Types, want) {
		t.Fatalf("types = %#v, want %#v", statuses[0].Types, want)
	}
}

func TestTunerStatusStreamInfoIsSnapshot(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "test", Types: []string{"GR"}, Command: "true"},
	}})
	item := mgr.tuners[0]
	input := map[string]StreamInfo{"GR/27": {Packet: 1}}
	mgr.addUser(item, User{ID: "viewer", StreamInfo: input})

	status, ok := mgr.Status(0)
	if !ok {
		t.Fatal("status not found")
	}
	mgr.updateUserStreamInfo(item, "viewer", "GR/27", StreamInfo{Packet: 2})
	input["GR/27"] = StreamInfo{Packet: 3}

	if got := status.Users[0].StreamInfo["GR/27"].Packet; got != 1 {
		t.Fatalf("snapshot packet = %d, want 1", got)
	}
	current, _ := mgr.Status(0)
	if got := current.Users[0].StreamInfo["GR/27"].Packet; got != 2 {
		t.Fatalf("current packet = %d, want 2", got)
	}

	status.Users[0].StreamInfo["GR/27"] = StreamInfo{Packet: 4}
	current, _ = mgr.Status(0)
	if got := current.Users[0].StreamInfo["GR/27"].Packet; got != 2 {
		t.Fatalf("current packet after snapshot mutation = %d, want 2", got)
	}
}

func TestTunerStatusOmitsProcessFieldsForNonProcessDevice(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "test", Types: []string{"GR"}, Command: "sleep 1"},
	}})
	item := mgr.tuners[0]
	mgr.runtime[item].device = fakeStatusDevice{done: make(chan struct{})}

	status, ok := mgr.Status(0)
	if !ok {
		t.Fatal("status not found")
	}
	if status.Command != "" || status.PID != 0 {
		t.Fatalf("process fields = %q/%d, want empty/0", status.Command, status.PID)
	}
}

type fakeStatusDevice struct {
	done chan struct{}
}

func (d fakeStatusDevice) Start(context.Context, io.Writer) error { return nil }

func (d fakeStatusDevice) Stop(context.Context) error { return nil }

func (d fakeStatusDevice) Done() <-chan struct{} { return d.done }

func (d fakeStatusDevice) Err() error { return nil }
