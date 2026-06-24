package api

import (
	"os"
	"strings"
	"testing"
)

func TestOpenAPIDoesNotExposeContainerHostileOperations(t *testing.T) {
	data, err := os.ReadFile("api.yml")
	if err != nil {
		t.Fatal(err)
	}
	spec := string(data)
	for _, operationID := range []string{
		"getChannelsConfig",
		"updateChannelsConfig",
		"getServerConfig",
		"updateServerConfig",
		"getTunersConfig",
		"updateTunersConfig",
		"channelScan",
		"getChannelScanStatus",
		"stopChannelScan",
		"updateVersion",
		"restart",
	} {
		if strings.Contains(spec, "operationId: "+operationID) {
			t.Fatalf("api.yml exposes excluded operationId %q", operationID)
		}
	}
}
