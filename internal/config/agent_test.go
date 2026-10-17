package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentTunerConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, extra string
		valid       bool
	}{
		{"agent", "agent:\n    name: living\n    command: recpt1 <channel> - -", true},
		{"local command conflict", "command: recpt1\n  agent:\n    name: living\n    command: remote-recpt1", false},
		{"missing remote command", "agent:\n    name: living", false},
		{"misspelled agent", "agnet:\n    name: living\n    command: remote-recpt1", false},
		{"retry overflow", "agent:\n    name: living\n    command: recpt1\n  startupRetryMax: 101", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tuners.yml")
			if err := os.WriteFile(path, []byte("- name: remote\n  types: [CATV]\n  "+tc.extra+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadAndParseTunersConfig(path)
			if (err == nil) != tc.valid {
				t.Fatalf("error=%v, valid=%v", err, tc.valid)
			}
		})
	}
}

func TestAgentCredentialsValidation(t *testing.T) {
	hash := strings.Repeat("a", 64)
	for _, data := range []string{
		"agents:\n  - name: living\n    tokenHash: wrong\n",
		"agents:\n  - name: living\n    tokenHash: " + hash + "\n  - name: other\n    tokenHash: " + hash + "\n",
	} {
		path := filepath.Join(t.TempDir(), "server.yml")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadAndParseSystemConfig(path); err == nil {
			t.Fatal("invalid credentials accepted")
		}
	}
}

func TestDuplicateLocalTunerNamesRemainCompatible(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tuners.yml")
	data := "- name: tuner\n  types: [GR]\n  command: first\n- name: tuner\n  types: [GR]\n  command: second\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAndParseTunersConfig(path); err != nil {
		t.Fatal(err)
	}
}
