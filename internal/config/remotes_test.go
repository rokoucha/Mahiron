package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadAndParseRemotesConfig(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    RemotesConfig
		wantErr bool
	}{
		{
			name: "valid",
			path: "testdata/remotes-valid.yml",
			want: RemotesConfig{
				{Name: "living", URL: "http://living.local:40772/api"},
				{Name: "private", URL: "http://private.local:40772/api", BasicAuth: &BasicAuthConfig{Username: "user", Password: "pass"}, AvailabilityTimeout: 10000},
			},
		},
		{name: "empty name", path: "testdata/remotes-empty-name.yml", wantErr: true},
		{name: "empty basic password", path: "testdata/remotes-empty-basic-password.yml", wantErr: true},
		{name: "negative availability timeout", path: "testdata/remotes-negative-availability-timeout.yml", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAndParseRemotesConfig(tt.path)
			if (err != nil) != tt.wantErr {
				t.Fatalf("LoadAndParseRemotesConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("LoadAndParseRemotesConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadRemotesForChannels(t *testing.T) {
	no := false
	channels := ChannelsConfig{
		{
			Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no,
			Routes: []ChannelRouteConfig{
				{Id: "remote", Remote: "living", Type: "GR", Channel: "27", IsDisabled: &no},
			},
		},
	}

	remotes, err := loadRemotesForChannels("testdata/remotes-valid.yml", channels)
	if err != nil {
		t.Fatal(err)
	}
	if remotes.Get("living") == nil {
		t.Fatal("living remote not loaded")
	}

	if _, err := loadRemotesForChannels("testdata/missing-remotes.yml", ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27"}}); err != nil {
		t.Fatalf("missing remotes without remote routes should be allowed: %v", err)
	}
	if _, err := loadRemotesForChannels("testdata/remotes-valid.yml", ChannelsConfig{{
		Name: "NHK", Type: "GR", Channel: "27", Routes: []ChannelRouteConfig{{Remote: "missing", Type: "GR", Channel: "27"}},
	}}); err == nil {
		t.Fatal("undefined remote route should fail")
	}
}
