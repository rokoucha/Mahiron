package tuner

import (
	"fmt"
	"regexp"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/util"
)

var newProcess = util.NewProcess

var commandTemplatePattern = regexp.MustCompile(`(?i)<([a-z0-9_.-]+)>`)

func replaceCommandTemplate(template string, channel *config.ChannelConfig) string {
	if channel == nil {
		return commandTemplatePattern.ReplaceAllString(template, "")
	}

	vars := map[string]any{
		"channel":  channel.Channel,
		"type":     channel.Type,
		"satelite": "",
		"space":    0,
	}
	if satellite, ok := channel.CommandVars["satellite"]; ok {
		vars["satelite"] = satellite
	}
	for key, value := range channel.CommandVars {
		vars[key] = value
	}

	return commandTemplatePattern.ReplaceAllStringFunc(template, func(match string) string {
		submatches := commandTemplatePattern.FindStringSubmatch(match)
		if len(submatches) != 2 {
			return ""
		}
		if value, ok := vars[submatches[1]]; ok {
			return fmt.Sprint(value)
		}
		return ""
	})
}
