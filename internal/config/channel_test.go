package config

import "testing"

func TestLoadAndParseChannelsConfigNormalizesDefaults(t *testing.T) {
	got, err := LoadAndParseChannelsConfig("testdata/channels-valid.yml")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("channels = %d, want 6", len(got))
	}

	first := got[0]
	if first.Name != "Channel1" || first.Type != "GR" || first.Channel != "GR01" {
		t.Fatalf("first channel = %#v", first)
	}
	if first.IsDisabled == nil || *first.IsDisabled {
		t.Fatalf("first IsDisabled = %v, want false", first.IsDisabled)
	}
	if len(first.CommandVars) != 0 {
		t.Fatalf("first CommandVars = %#v, want empty", first.CommandVars)
	}
	if len(first.Routes) != 0 {
		t.Fatalf("first routes = %#v, want implicit default route", first.Routes)
	}
	routes := first.RoutesOrDefault()
	if len(routes) != 1 || routes[0].Id != "default" || routes[0].Type != first.Type || routes[0].Channel != first.Channel {
		t.Fatalf("effective routes = %#v, want default route matching channel", routes)
	}

	legacy := got[4]
	if legacy.Satelite != nil || legacy.Satellite != nil || legacy.Space != nil || legacy.Freq != nil || legacy.Polarity != nil {
		t.Fatalf("legacy fields were not normalized away: %#v", legacy)
	}
	if legacy.CommandVars["satellite"] != "SOMESAT" || legacy.CommandVars["space"] != uint8(1) ||
		legacy.CommandVars["freq"] != uint32(12345) || legacy.CommandVars["polarity"] != "H" {
		t.Fatalf("legacy command vars = %#v", legacy.CommandVars)
	}
}

func TestLoadAndParseChannelsConfigRoutes(t *testing.T) {
	got, err := LoadAndParseChannelsConfig("testdata/channels-routes.yml")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Routes) != 2 {
		t.Fatalf("channels = %#v, want one channel with two routes", got)
	}
	direct := got[0].Routes[0]
	if direct.Id != "bs-direct" || direct.Remote != "" || direct.Type != "BS" || direct.Channel != "101" {
		t.Fatalf("direct route = %#v", direct)
	}
	remote := got[0].Routes[1]
	if remote.Id != "catv-bs-transmod" || remote.Remote != "living" || remote.Type != "CATV_BS" || remote.Channel != "C101" {
		t.Fatalf("remote route = %#v", remote)
	}
	if remote.Priority == nil || *remote.Priority != 20 {
		t.Fatalf("remote route priority = %v, want 20", remote.Priority)
	}
	if remote.CommandVars["freq"] != 12345.0 {
		t.Fatalf("remote route command vars = %#v", remote.CommandVars)
	}
}

func TestLoadAndParseChannelsConfigRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "empty config", path: "testdata/empty.yml"},
		{name: "empty name", path: "testdata/channels-empty-name.yml"},
		{name: "empty type", path: "testdata/channels-empty-type.yml"},
		{name: "empty symbol", path: "testdata/channels-empty-symbol.yml"},
		{name: "tsmfRelTs without serviceId", path: "testdata/channels-tsmfrelts.yml"},
		{name: "invalid tsmfRelTs", path: "testdata/channels-invalid-tsmfrelts.yml"},
		{name: "legacy fields with commandVars", path: "testdata/channels-duplicate-commandvars.yml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := LoadAndParseChannelsConfig(tt.path); err == nil {
				t.Fatal("LoadAndParseChannelsConfig() error = nil, want error")
			}
		})
	}
}
