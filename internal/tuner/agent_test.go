package tuner

import (
	"context"
	"errors"
	"testing"

	"github.com/21S1298001/mahiron/internal/agent"
	"github.com/21S1298001/mahiron/internal/config"
)

func TestOfflineAgentDoesNotHideLocalTuner(t *testing.T) {
	mgr := NewManager(&ManagerConfig{Agents: agent.NewHub(nil), TunersConfig: config.TunersConfig{
		{Name: "remote", Types: []string{"GR"}, Agent: &config.AgentTunerConfig{Name: "living", Command: "remote"}},
		{Name: "local", Types: []string{"GR"}, Command: "local"},
	}})
	if err := mgr.CheckAvailable(context.Background(), "GR"); err != nil {
		t.Fatal(err)
	}
	d, _, err := mgr.AcquireDevice(context.Background(), "GR", nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if d.(*managedDevice).tuner.Name() != "local" {
		t.Fatal("offline agent selected")
	}
	if s, _ := mgr.Status(0); s.IsAvailable {
		t.Fatal("offline agent reported available")
	}
}

func TestOnlyOfflineAgentIsUnavailable(t *testing.T) {
	mgr := NewManager(&ManagerConfig{Agents: agent.NewHub(nil), TunersConfig: config.TunersConfig{
		{Name: "remote", Types: []string{"GR"}, Agent: &config.AgentTunerConfig{Name: "living", Command: "remote"}},
	}})
	_, _, err := mgr.AcquireDevice(context.Background(), "GR", nil, nil, false)
	if !errors.Is(err, ErrTunerUnavailable) {
		t.Fatalf("error=%v", err)
	}
}
