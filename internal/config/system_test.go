package config

import (
	"testing"

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
				LogLevel:           "info",
				Observability:      ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:  1,
				DatabasePath:       "./db/mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherTimeout:  1200000,
				ServiceScanTimeout: 30000,
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
				LogLevel:           "info",
				Observability:      ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:  1,
				DatabasePath:       "./db/mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherTimeout:  1200000,
				ServiceScanTimeout: 30000,
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
				LogLevel:           "info",
				Observability:      ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:  1,
				DatabasePath:       "./db/mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherTimeout:  1200000,
				ServiceScanTimeout: 30000,
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
				LogLevel:           "debug",
				Observability:      ObservabilityConfig{ServiceName: "mahiron"},
				MaxConcurrentJobs:  1,
				DatabasePath:       "./db/mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherTimeout:  1200000,
				ServiceScanTimeout: 30000,
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
				MaxConcurrentJobs:  1,
				DatabasePath:       "./db/mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherTimeout:  1200000,
				ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured logo gather timeout",
			args: args{filePath: "testdata/system-logo-gather-timeout.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 1, DatabasePath: "./db/mahiron.db",
				EpgRetentionDays: 3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 600000, ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured max concurrent jobs",
			args: args{filePath: "testdata/system-max-concurrent-jobs.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 4, DatabasePath: "./db/mahiron.db",
				EpgRetentionDays: 3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 1200000, ServiceScanTimeout: 30000,
			},
		},
		{
			name: "Configured service scan timeout",
			args: args{filePath: "testdata/system-service-scan-timeout.yml"},
			want: &SystemConfig{
				Addresses: []ServerAddress{{Http: "localhost:40772"}}, LogLevel: "info",
				Observability: ObservabilityConfig{ServiceName: "mahiron"}, MaxConcurrentJobs: 1, DatabasePath: "./db/mahiron.db",
				EpgRetentionDays: 3, EpgRetrievalTime: 600000, EpgStaleAfter: 7200000, LogoGatherTimeout: 1200000, ServiceScanTimeout: 45000,
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
