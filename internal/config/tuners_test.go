package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadAndParseTunersConfig(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    TunersConfig
		wantErr bool
	}{
		{
			name: "Valid config",
			args: args{
				filePath: "testdata/tuners-valid.yml",
			},
			want: TunersConfig{
				{
					Name:          "Tuner1",
					Types:         []string{"GR"},
					Command:       "echo \"Hello World\"",
					DvbDevicePath: "",
					Decoder:       "test",
					IsDisabled:    false,
					Remote:        nil,
				},
				{
					Name:          "Tuner2",
					Types:         []string{"SKY"},
					Command:       "echo \"Hello World\"",
					DvbDevicePath: "/dev/dvb/adapter0",
					Decoder:       "",
					IsDisabled:    false,
					Remote:        nil,
				},
				{
					Name:          "Tuner4",
					Types:         []string{"CATV_BS", "BS"},
					Command:       "",
					DvbDevicePath: "",
					Decoder:       "",
					IsDisabled:    false,
					Remote: &Remote{
						Url:   "http://localhost:40772/api",
						Types: map[string]string{"BS": "BS", "CATV_BS": "SKY"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Empty file",
			args: args{
				filePath: "testdata/empty.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty tuner name",
			args: args{
				filePath: "testdata/tuners-empty-name.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty tuner source",
			args: args{
				filePath: "testdata/tuners-empty-source.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty tuner types",
			args: args{
				filePath: "testdata/tuners-empty-grouping.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Multiple tuner sources",
			args: args{
				filePath: "testdata/tuners-multiple-sources.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Only specified dvbDevicePath",
			args: args{
				filePath: "testdata/tuners-dvb-path.yml",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAndParseTunersConfig(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAndParseTunersConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("LoadAndParseTunersConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
