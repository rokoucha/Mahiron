package config

import (
	"reflect"
	"testing"
)

func TestLoadAndParseServerConfig(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    *ServerConfig
		wantErr bool
	}{
		{
			name: "Empty file",
			args: args{
				filePath: "testdata/empty.yml",
			},
			want: &ServerConfig{
				Addresses: []ServerAddress{
					{
						Http: "localhost:40772",
					},
				},
				LogLevel: "info",
			},
			wantErr: false,
		},
		{
			name: "Multiple http addresses",
			args: args{
				filePath: "testdata/server-multiple-http.yml",
			},
			want: &ServerConfig{
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
				LogLevel: "info",
			},
			wantErr: false,
		},
		{
			name: "Multiple unix addresses",
			args: args{
				filePath: "testdata/server-multiple-unix.yml",
			},
			want: &ServerConfig{
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
				LogLevel: "info",
			},
			wantErr: false,
		},
		{
			name: "Both http and unix addresses",
			args: args{
				filePath: "testdata/server-http-and-unix.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Verbose log level",
			args: args{
				filePath: "testdata/server-verbose.yml",
			},
			want: &ServerConfig{
				Addresses: []ServerAddress{
					{
						Http: "localhost:40772",
					},
				},
				LogLevel: "debug",
			},
			wantErr: false,
		},
		{
			name: "Invalid log level",
			args: args{
				filePath: "testdata/server-invalid-log-level.yml",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAndParseServerConfig(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAndParseServerConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadAndParseServerConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
