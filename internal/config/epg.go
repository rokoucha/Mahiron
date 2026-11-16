package config

import "errors"

type EPGMode string

const (
	EPGContinuous EPGMode = "continuous"
	EPGScheduled  EPGMode = "scheduled"
	EPGDisabled   EPGMode = "disabled"

	ServiceUpdaterKey = "service-updater"
	LogoGathererKey   = "logo-gatherer"
	EPGGathererKey    = "epg-gatherer"

	ServiceUpdaterDefaultSchedule = "5 6 * * *"
	LogoGathererDefaultSchedule   = "5 3 * * *"
	EPGGathererDefaultSchedule    = "20,50 * * * *"
)

type EPGConfig struct {
	Mode      EPGMode
	Schedules []JobScheduleConfig
}

// ResolveEPG validates mode/schedule compatibility and supplies default schedules.
// Explicit schedules, including omission of EPG, are preserved.
func (c SystemConfig) ResolveEPG() (EPGConfig, error) {
	mode := c.EPGMode()
	switch mode {
	case EPGContinuous, EPGScheduled, EPGDisabled:
	default:
		return EPGConfig{}, errors.New("epgGatheringMode must be continuous, scheduled or disabled")
	}
	hasEPG := false
	for _, schedule := range c.Jobs {
		if schedule.Key == EPGGathererKey {
			hasEPG = true
		}
	}
	if hasEPG && mode != EPGScheduled {
		return EPGConfig{}, errors.New("remove epg-gatherer cron when using continuous or disabled EPG gathering")
	}
	if mode == EPGScheduled && len(c.Jobs) > 0 && !hasEPG {
		return EPGConfig{}, errors.New("scheduled EPG gathering requires an epg-gatherer entry in jobs")
	}
	schedules := append([]JobScheduleConfig(nil), c.Jobs...)
	if len(schedules) == 0 {
		schedules = []JobScheduleConfig{
			{Key: ServiceUpdaterKey, Schedule: ServiceUpdaterDefaultSchedule},
			{Key: LogoGathererKey, Schedule: LogoGathererDefaultSchedule},
		}
		if mode == EPGScheduled {
			schedules = append(schedules, JobScheduleConfig{Key: EPGGathererKey, Schedule: EPGGathererDefaultSchedule})
		}
	}
	return EPGConfig{Mode: mode, Schedules: schedules}, nil
}

// EPGMode preserves explicitly configured schedules, including omission of EPG.
func (c SystemConfig) EPGMode() EPGMode {
	if c.EpgGatheringMode != "" {
		return c.EpgGatheringMode
	}
	if len(c.Jobs) == 0 {
		return EPGContinuous
	}
	for _, schedule := range c.Jobs {
		if schedule.Key == EPGGathererKey {
			return EPGScheduled
		}
	}
	return EPGDisabled
}
