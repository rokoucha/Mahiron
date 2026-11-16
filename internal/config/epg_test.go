package config

import (
	"reflect"
	"testing"
)

func TestResolveEPG(t *testing.T) {
	epg := JobScheduleConfig{Key: "epg-gatherer", Schedule: "15 * * * *"}
	other := JobScheduleConfig{Key: "logo-gatherer", Schedule: "10 * * * *"}
	defaults := []JobScheduleConfig{
		{Key: "service-updater", Schedule: ServiceUpdaterDefaultSchedule},
		{Key: "logo-gatherer", Schedule: LogoGathererDefaultSchedule},
	}
	for _, tt := range []struct {
		name       string
		configured EPGMode
		jobs       []JobScheduleConfig
		mode       EPGMode
		schedules  []JobScheduleConfig
		invalid    bool
	}{
		{name: "implicit continuous", mode: EPGContinuous, schedules: defaults},
		{name: "continuous", configured: EPGContinuous, mode: EPGContinuous, schedules: defaults},
		{name: "scheduled defaults", configured: EPGScheduled, mode: EPGScheduled, schedules: append(append([]JobScheduleConfig(nil), defaults...), JobScheduleConfig{Key: "epg-gatherer", Schedule: EPGGathererDefaultSchedule})},
		{name: "disabled defaults", configured: EPGDisabled, mode: EPGDisabled, schedules: defaults},
		{name: "implicit scheduled", jobs: []JobScheduleConfig{epg}, mode: EPGScheduled, schedules: []JobScheduleConfig{epg}},
		{name: "implicit disabled", jobs: []JobScheduleConfig{other}, mode: EPGDisabled, schedules: []JobScheduleConfig{other}},
		{name: "continuous custom", configured: EPGContinuous, jobs: []JobScheduleConfig{other}, mode: EPGContinuous, schedules: []JobScheduleConfig{other}},
		{name: "scheduled custom", configured: EPGScheduled, jobs: []JobScheduleConfig{epg, other}, mode: EPGScheduled, schedules: []JobScheduleConfig{epg, other}},
		{name: "unknown", configured: "invalid", invalid: true},
		{name: "scheduled missing EPG", configured: EPGScheduled, jobs: []JobScheduleConfig{other}, invalid: true},
		{name: "continuous with cron", configured: EPGContinuous, jobs: []JobScheduleConfig{epg}, invalid: true},
		{name: "disabled with cron", configured: EPGDisabled, jobs: []JobScheduleConfig{epg}, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := SystemConfig{EpgGatheringMode: tt.configured, Jobs: tt.jobs}
			before := append([]JobScheduleConfig(nil), c.Jobs...)
			got, err := c.ResolveEPG()
			if (err != nil) != tt.invalid {
				t.Fatalf("ResolveEPG error = %v, invalid = %v", err, tt.invalid)
			}
			if tt.invalid {
				return
			}
			if got.Mode != tt.mode || !reflect.DeepEqual(got.Schedules, tt.schedules) {
				t.Fatalf("ResolveEPG = %#v, want mode %s schedules %v", got, tt.mode, tt.schedules)
			}
			got.Schedules[0].Schedule = "changed"
			if !reflect.DeepEqual(c.Jobs, before) {
				t.Fatal("ResolveEPG modified configured jobs")
			}
		})
	}
}
