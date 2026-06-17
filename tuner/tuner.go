package tuner

import "github.com/21S1298001/Mahiron5/config"

type Tuner struct {
	config *config.TunerConfig
}

func NewTuner(config *config.TunerConfig) *Tuner {
	return &Tuner{
		config: config,
	}
}

func (t *Tuner) Name() string {
	return t.config.Name
}

func (t *Tuner) Groups() []string {
	groups := map[string]struct{}{}
	for _, group := range t.config.Types {
		groups[group] = struct{}{}
	}

	groupList := make([]string, 0, len(groups))
	for group := range groups {
		groupList = append(groupList, group)
	}
	return groupList
}

func (t *Tuner) IsDisabled() bool {
	return t.config.IsDisabled
}

func (t *Tuner) Command() string {
	return t.config.Command
}

func (t *Tuner) DecoderCommand() string {
	return t.config.Decoder
}

func (t *Tuner) NewDevice(channel *config.ChannelConfig) Device {
	return NewTunerDevice(TunerDeviceConfig{
		Channel: channel,
		Command: t.config.Command,
	})
}
