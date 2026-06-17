package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadAndParseChannelsConfig(t *testing.T) {
	yes := true
	no := false

	ch2ServiceId := uint32(25565)

	ch3ServiceId := uint32(12345)
	ch3tsmfRelTs := uint8(15)

	ch5ServiceId := uint32(65534)
	routePriority10 := 10
	routePriority20 := 20

	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    ChannelsConfig
		wantErr bool
	}{
		{
			name: "Valid config",
			args: args{
				filePath: "testdata/channels-valid.yml",
			},
			want: ChannelsConfig{
				{
					Name:        "Channel1",
					Type:        "GR",
					Channel:     "GR01",
					ServiceId:   nil,
					TsmfRelTs:   nil,
					CommandVars: map[string]any{},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					Routes: []ChannelRouteConfig{
						{Id: "default", Type: "GR", Channel: "GR01", CommandVars: map[string]any{}, IsDisabled: &no},
					},
				},
				{
					Name:        "Channel2",
					Type:        "SKY",
					Channel:     "SKY02",
					ServiceId:   &ch2ServiceId,
					TsmfRelTs:   nil,
					CommandVars: map[string]any{},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					Routes: []ChannelRouteConfig{
						{Id: "default", Type: "SKY", Channel: "SKY02", ServiceId: &ch2ServiceId, CommandVars: map[string]any{}, IsDisabled: &no},
					},
				},
				{
					Name:        "Channel3",
					Type:        "CATV",
					Channel:     "CATV03",
					ServiceId:   &ch3ServiceId,
					TsmfRelTs:   &ch3tsmfRelTs,
					CommandVars: map[string]any{},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					Routes: []ChannelRouteConfig{
						{Id: "default", Type: "CATV", Channel: "CATV03", ServiceId: &ch3ServiceId, TsmfRelTs: &ch3tsmfRelTs, CommandVars: map[string]any{}, IsDisabled: &no},
					},
				},
				{
					Name:      "Channel4",
					Type:      "BS",
					Channel:   "BS04",
					ServiceId: nil,
					TsmfRelTs: nil,
					CommandVars: map[string]any{
						"extra-args": "--extra-arg",
					},
					IsDisabled: &no,
					Satelite:   nil,
					Satellite:  nil,
					Space:      nil,
					Freq:       nil,
					Polarity:   nil,
					Routes: []ChannelRouteConfig{
						{Id: "default", Type: "BS", Channel: "BS04", CommandVars: map[string]any{"extra-args": "--extra-arg"}, IsDisabled: &no},
					},
				},
				{
					Name:      "Channel5",
					Type:      "CS",
					Channel:   "CS05",
					ServiceId: &ch5ServiceId,
					TsmfRelTs: nil,
					CommandVars: map[string]any{
						"satellite": "SOMESAT",
						"space":     uint8(1),
						"freq":      uint32(12345),
						"polarity":  "H",
					},
					IsDisabled: &no,
					Satelite:   nil,
					Satellite:  nil,
					Space:      nil,
					Freq:       nil,
					Polarity:   nil,
					Routes: []ChannelRouteConfig{
						{
							Id:        "default",
							Type:      "CS",
							Channel:   "CS05",
							ServiceId: &ch5ServiceId,
							CommandVars: map[string]any{
								"satellite": "SOMESAT",
								"space":     uint8(1),
								"freq":      uint32(12345),
								"polarity":  "H",
							},
							IsDisabled: &no,
						},
					},
				},
				{
					Name:        "Channel6",
					Type:        "CATV",
					Channel:     "CATV06",
					ServiceId:   nil,
					TsmfRelTs:   nil,
					CommandVars: map[string]any{},
					IsDisabled:  &yes,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					Routes: []ChannelRouteConfig{
						{Id: "default", Type: "CATV", Channel: "CATV06", CommandVars: map[string]any{}, IsDisabled: &yes},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Empty config",
			args: args{
				filePath: "testdata/empty.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Routes config",
			args: args{
				filePath: "testdata/channels-routes.yml",
			},
			want: ChannelsConfig{
				{
					Name:        "NHK BS",
					Type:        "BS",
					Channel:     "101",
					CommandVars: map[string]any{},
					IsDisabled:  &no,
					Routes: []ChannelRouteConfig{
						{Id: "bs-direct", Type: "BS", Channel: "101", CommandVars: map[string]any{}, IsDisabled: &no, Priority: &routePriority10},
						{
							Id:          "catv-bs-transmod",
							Type:        "CATV_BS",
							Channel:     "C101",
							CommandVars: map[string]any{"freq": 12345.0},
							IsDisabled:  &no,
							Priority:    &routePriority20,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Empty tuner name",
			args: args{
				filePath: "testdata/channels-empty-name.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty channel type",
			args: args{
				filePath: "testdata/channels-empty-type.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty channel symbol",
			args: args{
				filePath: "testdata/channels-empty-symbol.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Only specified tsmfRelTs",
			args: args{
				filePath: "testdata/channels-tsmfrelts.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid tsmfRelTs",
			args: args{
				filePath: "testdata/channels-invalid-tsmfrelts.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Duplicate specify commandVars and other fields",
			args: args{
				filePath: "testdata/channels-duplicate-commandvars.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Specified tunerGroups",
			args: args{
				filePath: "testdata/channels-tuner-groups.yml",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAndParseChannelsConfig(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAndParseChannelsConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("LoadAndParseChannelsConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
