package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestLoadAndParseSystemConfig(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    *SystemConfig
		wantErr bool
	}{
		{
			name: "Empty file",
			args: args{
				filePath: "testdata/empty.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Http: "localhost:40772",
					},
				},
				LogLevel:                         "info",
				Observability:                    ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:                1,
				DatabasePath:                     "./db/mahiron.db",
				DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
				DataBroadcastCacheBytes:          1024 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     14,
				DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3,
				EpgRetrievalTime:                 600000,
				EpgStaleAfter:                    7200000,
				LogoGatherTimeout:                1200000,
				ServiceScanTimeout:               30000,
			},
			wantErr: false,
		},
		{
			name: "Multiple http addresses",
			args: args{
				filePath: "testdata/system-multiple-http.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Http: "test:1",
					},
					{
						Http: "test:2",
					},
					{
						Http: "test:3",
					},
				},
				LogLevel:                         "info",
				Observability:                    ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:                1,
				DatabasePath:                     "./db/mahiron.db",
				DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
				DataBroadcastCacheBytes:          1024 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     14,
				DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3,
				EpgRetrievalTime:                 600000,
				EpgStaleAfter:                    7200000,
				LogoGatherTimeout:                1200000,
				ServiceScanTimeout:               30000,
			},
			wantErr: false,
		},
		{
			name: "Multiple unix addresses",
			args: args{
				filePath: "testdata/system-multiple-unix.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Unix: "/test1.sock",
					},
					{
						Unix: "/test2.sock",
					},
					{
						Unix: "/test3.sock",
					},
				},
				LogLevel:                         "info",
				Observability:                    ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:                1,
				DatabasePath:                     "./db/mahiron.db",
				DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
				DataBroadcastCacheBytes:          1024 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     14,
				DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3,
				EpgRetrievalTime:                 600000,
				EpgStaleAfter:                    7200000,
				LogoGatherTimeout:                1200000,
				ServiceScanTimeout:               30000,
			},
			wantErr: false,
		},
		{
			name: "Both http and unix addresses",
			args: args{
				filePath: "testdata/system-http-and-unix.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Verbose log level",
			args: args{
				filePath: "testdata/system-verbose.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Http: "localhost:40772",
					},
				},
				LogLevel:                         "debug",
				Observability:                    ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:                1,
				DatabasePath:                     "./db/mahiron.db",
				DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
				DataBroadcastCacheBytes:          1024 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     14,
				DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3,
				EpgRetrievalTime:                 600000,
				EpgStaleAfter:                    7200000,
				LogoGatherTimeout:                1200000,
				ServiceScanTimeout:               30000,
			},
			wantErr: false,
		},
		{
			name: "Invalid log level",
			args: args{
				filePath: "testdata/system-invalid-log-level.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Observability endpoint without scheme",
			args: args{
				filePath: "testdata/system-observability-invalid-endpoint.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Configured observability",
			args: args{filePath: "testdata/system-observability.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}},
				LogLevel:  "info",
				Observability: ObservabilityConfig{
					ServiceName: "custom-mahiron",
					Endpoint:    "http://localhost:4318",
					Headers: map[string]string{
						"authorization": "Bearer token",
						"x-tenant":      "test",
					},
					Logs:   ObservabilitySignal{Enabled: true},
					Traces: ObservabilitySignal{Enabled: false},
					Metrics: ObservabilitySignal{
						Enabled: true,
					},
				},
				MaxConcurrentJobs:                1,
				DatabasePath:                     "./db/mahiron.db",
				DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
				DataBroadcastCacheBytes:          1024 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     14,
				DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3,
				EpgRetrievalTime:                 600000,
				EpgStaleAfter:                    7200000,
				LogoGatherTimeout:                1200000,
				ServiceScanTimeout:               30000,
			},
		},
		{
			name: "Configured logo gather timeout",
			args: args{filePath: "testdata/system-logo-gather-timeout.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 1, DatabasePath: "./db/mahiron.db", DataBroadcastCachePath: "./db/data-broadcast-cache.db", DataBroadcastCacheBytes: 1024 * 1024 * 1024, DataBroadcastCacheMaxAgeDays: 14, DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime: 30000,
				EpgExtendedProbeInterval:   21600000,
				EpgRefreshInterval:         1800000,
				EpgRetentionDays:           3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 600000, ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured data broadcast cache size",
			args: args{filePath: "testdata/system-data-broadcast-cache-size.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 1, DatabasePath: "./db/mahiron.db",
				DataBroadcastCachePath:           "/var/cache/mahiron/data-broadcast.db",
				DataBroadcastCacheBytes:          32 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     2,
				DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 1200000, ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured data broadcast snapshot disabled",
			args: args{filePath: "testdata/system-data-broadcast-snapshot-disabled.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 1, DatabasePath: "./db/mahiron.db",
				DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
				DataBroadcastCacheBytes:          1024 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     14,
				DataBroadcastSnapshotDisabled:    boolPtr(true),
				DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 1200000, ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured data broadcast snapshot max age",
			args: args{filePath: "testdata/system-data-broadcast-snapshot-max-age.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 1, DatabasePath: "./db/mahiron.db",
				DataBroadcastCachePath:           "./db/data-broadcast-cache.db",
				DataBroadcastCacheBytes:          1024 * 1024 * 1024,
				DataBroadcastCacheMaxAgeDays:     14,
				DataBroadcastSnapshotMaxAgeHours: 1,
				EpgUnknownExtendedIdleTime:       30000,
				EpgExtendedProbeInterval:         21600000,
				EpgRefreshInterval:               1800000,
				EpgRetentionDays:                 3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 1200000, ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured max concurrent jobs",
			args: args{filePath: "testdata/system-max-concurrent-jobs.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 4, DatabasePath: "./db/mahiron.db", DataBroadcastCachePath: "./db/data-broadcast-cache.db", DataBroadcastCacheBytes: 1024 * 1024 * 1024, DataBroadcastCacheMaxAgeDays: 14, DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime: 30000,
				EpgExtendedProbeInterval:   21600000,
				EpgRefreshInterval:         1800000,
				EpgRetentionDays:           3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 1200000, ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured service scan timeout",
			args: args{filePath: "testdata/system-service-scan-timeout.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 1, DatabasePath: "./db/mahiron.db", DataBroadcastCachePath: "./db/data-broadcast-cache.db", DataBroadcastCacheBytes: 1024 * 1024 * 1024, DataBroadcastCacheMaxAgeDays: 14, DataBroadcastSnapshotMaxAgeHours: 6,
				EpgUnknownExtendedIdleTime: 30000,
				EpgExtendedProbeInterval:   21600000,
				EpgRefreshInterval:         1800000,
				EpgRetentionDays:           3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 1200000, ServiceScanTimeout: 45000,
			},
		},
		{
			name:    "Invalid service scan timeout",
			args:    args{filePath: "testdata/system-invalid-service-scan-timeout.yml"},
			wantErr: true,
		},
		{
			name:    "Invalid max concurrent jobs",
			args:    args{filePath: "testdata/system-invalid-max-concurrent-jobs.yml"},
			wantErr: true,
		},
		{
			name:    "Invalid data broadcast cache age",
			args:    args{filePath: "testdata/system-invalid-data-broadcast-cache-age.yml"},
			wantErr: true,
		},
		{
			name:    "Invalid data broadcast snapshot max age",
			args:    args{filePath: "testdata/system-invalid-data-broadcast-snapshot-max-age.yml"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAndParseSystemConfig(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAndParseSystemConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("LoadAndParseSystemConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestIsDataBroadcastSnapshotEnabled(t *testing.T) {
	if !IsDataBroadcastSnapshotEnabled(SystemConfig{}) {
		t.Fatal("snapshot support should default to enabled when unset")
	}
	if !IsDataBroadcastSnapshotEnabled(SystemConfig{DataBroadcastSnapshotDisabled: boolPtr(false)}) {
		t.Fatal("snapshot support should be enabled when explicitly false")
	}
	if IsDataBroadcastSnapshotEnabled(SystemConfig{DataBroadcastSnapshotDisabled: boolPtr(true)}) {
		t.Fatal("snapshot support should be disabled when explicitly true")
	}
}

func TestIsDataBroadcastEnabled(t *testing.T) {
	if !IsDataBroadcastEnabled(SystemConfig{}) {
		t.Fatal("data broadcast API should default to enabled when unset")
	}
	if !IsDataBroadcastEnabled(SystemConfig{DataBroadcastEnabled: boolPtr(true)}) {
		t.Fatal("data broadcast API should be enabled when explicitly true")
	}
	if IsDataBroadcastEnabled(SystemConfig{DataBroadcastEnabled: boolPtr(false)}) {
		t.Fatal("data broadcast API should be disabled when explicitly false")
	}
}

func boolPtr(v bool) *bool { return &v }

func TestEPGModePreservesExplicitLegacySchedules(t *testing.T) {
	for _, tc := range []struct {
		config SystemConfig
		want   EPGMode
	}{
		{SystemConfig{}, "continuous"},
		{SystemConfig{Jobs: []JobScheduleConfig{{Key: "epg-gatherer", Schedule: "0 3 * * *"}}}, "scheduled"},
		{SystemConfig{Jobs: []JobScheduleConfig{{Key: "logo-gatherer", Schedule: "0 3 * * *"}}}, "disabled"},
		{SystemConfig{EpgGatheringMode: "continuous", Jobs: []JobScheduleConfig{{Key: "logo-gatherer", Schedule: "0 3 * * *"}}}, "continuous"},
	} {
		if got := tc.config.EPGMode(); got != tc.want {
			t.Fatalf("mode=%s want %s", got, tc.want)
		}
	}
}

func TestEPGModeRejectsAmbiguousSchedules(t *testing.T) {
	for _, body := range []string{
		"epgGatheringMode: scheduled\njobs:\n - key: logo-gatherer\n   schedule: '0 3 * * *'\n",
		"epgGatheringMode: continuous\njobs:\n - key: epg-gatherer\n   schedule: '0 3 * * *'\n",
		"epgRefreshInterval: 0\n",
	} {
		path := filepath.Join(t.TempDir(), "server.yml")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadAndParseSystemConfig(path); err == nil {
			t.Fatalf("invalid config accepted: %s", body)
		}
	}
}

func TestEPGReceptionBudgetValidation(t *testing.T) {
	for _, text := range []string{"epgUnknownExtendedIdleTime: -1\n", "epgUnknownExtendedIdleTime: 999\n", "epgExtendedProbeInterval: 0\n"} {
		path := filepath.Join(t.TempDir(), "server.yml")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadAndParseSystemConfig(path); err == nil {
			t.Fatalf("accepted invalid budget %q", text)
		}
	}
}

func TestSystemConfigDurations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.yml")
	if err := os.WriteFile(path, []byte(`epgUnknownExtendedIdleTime: 1234
epgExtendedProbeInterval: 2345
epgRefreshInterval: 3456
epgRetrievalTime: 5678
epgStaleAfter: 6789
logoGatherTimeout: 7890
serviceScanTimeout: 8901
`), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadAndParseSystemConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"unknown idle", config.EpgUnknownExtendedIdleTimeDuration(), 1234 * time.Millisecond},
		{"extended probe", config.EpgExtendedProbeIntervalDuration(), 2345 * time.Millisecond},
		{"refresh", config.EpgRefreshIntervalDuration(), 3456 * time.Millisecond},
		{"retrieval", config.EpgRetrievalTimeDuration(), 5678 * time.Millisecond},
		{"stale", config.EpgStaleAfterDuration(), 6789 * time.Millisecond},
		{"logo", config.LogoGatherTimeoutDuration(), 7890 * time.Millisecond},
		{"scan", config.ServiceScanTimeoutDuration(), 8901 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("duration = %v, want %v", test.got, test.want)
			}
		})
	}
}

func TestSystemConfigExplicitZeroAndEmptyDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.yml")
	if err := os.WriteFile(path, []byte(`addresses: []
logLevel: ""
observability:
  serviceName: ""
epgUnknownExtendedIdleTime: 0
epgRetentionDays: 0
dataBroadcastCacheBytes: 0
dataBroadcastCacheMaxAgeDays: 0
dataBroadcastSnapshotMaxAgeHours: 0
`), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadAndParseSystemConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]ServerAddress{{Http: "localhost:40772"}}, config.Addresses); diff != "" {
		t.Fatal(diff)
	}
	if config.LogLevel != "info" || config.Observability.ServiceName != "mahiron" {
		t.Fatalf("empty string defaults missing: %+v", config)
	}
	if config.EpgUnknownExtendedIdleTimeDuration() != 0 || config.EpgRetentionDays != 0 || config.DataBroadcastCacheBytes != 0 || config.DataBroadcastCacheMaxAgeDays != 0 || config.DataBroadcastSnapshotMaxAgeHours != 0 {
		t.Fatalf("explicit zero replaced by defaults: %+v", config)
	}
}

func TestSystemConfigNullPreservesNumericDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.yml")
	if err := os.WriteFile(path, []byte(`epgUnknownExtendedIdleTime: null
epgRetentionDays: null
dataBroadcastCacheBytes: null
dataBroadcastCacheMaxAgeDays: null
dataBroadcastSnapshotMaxAgeHours: null
maxConcurrentJobs: null
epgStaleAfter: null
logoGatherTimeout: null
`), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadAndParseSystemConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	want := defaultSystemConfig()
	want.applyDefaults()
	if diff := cmp.Diff(&want, config); diff != "" {
		t.Fatal(diff)
	}
}

func TestSystemConfigRejectsExplicitZeroDurationsAndConcurrency(t *testing.T) {
	for _, field := range []string{"maxConcurrentJobs", "epgStaleAfter", "logoGatherTimeout", "epgRetrievalTime", "serviceScanTimeout"} {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "server.yml")
			if err := os.WriteFile(path, []byte(field+": 0\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadAndParseSystemConfig(path); err == nil {
				t.Fatal("explicit zero accepted")
			}
		})
	}
}
