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
				Observability:      ObservabilityConfig{ServiceName: "mahiron5"},
				JobMaxRunning:      1,
				DatabasePath:       "./mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherDuration: 86400000,
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
				Observability:      ObservabilityConfig{ServiceName: "mahiron5"},
				JobMaxRunning:      1,
				DatabasePath:       "./mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherDuration: 86400000,
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
				Observability:      ObservabilityConfig{ServiceName: "mahiron5"},
				JobMaxRunning:      1,
				DatabasePath:       "./mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherDuration: 86400000,
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
				Observability:      ObservabilityConfig{ServiceName: "mahiron5"},
				JobMaxRunning:      1,
				DatabasePath:       "./mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherDuration: 86400000,
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
			name: "Configured job max running",
			args: args{filePath: "testdata/system-job-max-running.yml"},
			want: &SystemConfig{
				Addresses:          []ServerAddress{{Http: "localhost:40772"}},
				LogLevel:           "info",
				Observability:      ObservabilityConfig{ServiceName: "mahiron5"},
				JobMaxRunning:      4,
				DatabasePath:       "./mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherDuration: 86400000,
			},
		},
		{
			name:    "Invalid job max running",
			args:    args{filePath: "testdata/system-invalid-job-max-running.yml"},
			wantErr: true,
		},
		{
			name: "Configured observability",
			args: args{filePath: "testdata/system-observability.yml"},
			want: &SystemConfig{
				Addresses:     []ServerAddress{{Http: "localhost:40772"}},
				LogLevel:      "info",
				JobMaxRunning: 1,
				Observability: ObservabilityConfig{
					ServiceName: "custom-mahiron",
					Endpoint:    "localhost:4317",
					Insecure:    true,
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
				DatabasePath:       "./mahiron.db",
				EpgRetentionDays:   3,
				EpgRetrievalTime:   600000,
				EpgStaleAfter:      7200000,
				LogoGatherDuration: 86400000,
			},
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
