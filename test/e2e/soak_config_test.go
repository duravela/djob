package e2e_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

type soakConfig struct {
	CompactConfigurationReceipts bool          `json:"compact_configuration_receipts"`
	HistoryWindow                time.Duration `json:"payload_history_window_ns"`
	CycleInterval                time.Duration `json:"cycle_interval_ns"`
	GoMemoryMiB                  int           `json:"controller_go_memory_limit_mib"`
	MaxDatabaseMiB               int           `json:"max_database_mib_per_controller"`
	Duration                     time.Duration `json:"duration_ns"`
	BatchSize                    int           `json:"batch_size_per_tenant"`
	FaultEvery                   int           `json:"fault_every_cycles"`
	MaxLatency                   time.Duration `json:"max_observed_latency_ns"`
	MaxRSSMiB                    int           `json:"max_resident_mib_per_process"`
	MaxOpenFiles                 int           `json:"max_open_files_per_process"`
}

func loadSoakConfig(getenv func(string) (string, bool)) (soakConfig, error) {
	cfg := soakConfig{Duration: 30 * time.Second, BatchSize: 4, FaultEvery: 3, MaxLatency: 30 * time.Second}
	if text, ok := getenv("DJOB_SOAK_COMPACT_CONFIGURATION_RECEIPTS"); ok {
		value, err := strconv.ParseBool(text)
		if err != nil {
			return cfg, fmt.Errorf("DJOB_SOAK_COMPACT_CONFIGURATION_RECEIPTS must be a boolean")
		}
		cfg.CompactConfigurationReceipts = value
	}
	for _, setting := range []struct {
		name     string
		value    *time.Duration
		min, max time.Duration
	}{
		{"DJOB_SOAK_HISTORY_WINDOW", &cfg.HistoryWindow, 0, 24 * time.Hour},
		{"DJOB_SOAK_CYCLE_INTERVAL", &cfg.CycleInterval, 0, time.Minute},
		{"DJOB_SOAK_DURATION", &cfg.Duration, 10 * time.Second, 24 * time.Hour},
		{"DJOB_SOAK_MAX_LATENCY", &cfg.MaxLatency, time.Second, time.Minute},
	} {
		if text, ok := getenv(setting.name); ok {
			duration, err := time.ParseDuration(text)
			if err != nil || duration < setting.min || duration > setting.max {
				return cfg, fmt.Errorf("%s must be a duration from %s through %s", setting.name, setting.min, setting.max)
			}
			*setting.value = duration
		}
	}
	for _, setting := range []struct {
		name     string
		value    *int
		min, max int
	}{
		{"DJOB_SOAK_GO_MEMORY_MIB", &cfg.GoMemoryMiB, 0, 16384},
		{"DJOB_SOAK_MAX_DATABASE_MIB", &cfg.MaxDatabaseMiB, 0, 16384},
		{"DJOB_SOAK_BATCH_SIZE", &cfg.BatchSize, 1, 32},
		{"DJOB_SOAK_FAULT_EVERY", &cfg.FaultEvery, 0, 1000},
		{"DJOB_SOAK_MAX_RSS_MIB", &cfg.MaxRSSMiB, 0, 16384},
		{"DJOB_SOAK_MAX_OPEN_FILES", &cfg.MaxOpenFiles, 0, 1000000},
	} {
		if text, ok := getenv(setting.name); ok {
			n, err := strconv.Atoi(text)
			if err != nil || n < setting.min || n > setting.max {
				return cfg, fmt.Errorf("%s must be an integer from %d through %d", setting.name, setting.min, setting.max)
			}
			*setting.value = n
		}
	}
	if cfg.HistoryWindow > 0 && (cfg.HistoryWindow < time.Minute || cfg.Duration < cfg.HistoryWindow+10*time.Second) {
		return cfg, fmt.Errorf("history window must be at least 1m and leave at least 10s of qualification after its expiry")
	}
	if cfg.GoMemoryMiB > 0 && cfg.GoMemoryMiB < 32 {
		return cfg, fmt.Errorf("Go memory limit must be zero or at least 32 MiB")
	}
	if cfg.CompactConfigurationReceipts && cfg.HistoryWindow == 0 {
		return cfg, fmt.Errorf("configuration receipt maintenance requires an explicit history window")
	}
	return cfg, nil
}

func TestSoakConfiguration(t *testing.T) {
	cases := []struct {
		settings map[string]string
		valid    bool
	}{
		{map[string]string{}, true},
		{map[string]string{"DJOB_SOAK_DURATION": "2h", "DJOB_SOAK_FAULT_EVERY": "0"}, true},
		{map[string]string{"DJOB_SOAK_DURATION": "75s", "DJOB_SOAK_HISTORY_WINDOW": "1m", "DJOB_SOAK_GO_MEMORY_MIB": "256", "DJOB_SOAK_CYCLE_INTERVAL": "30s", "DJOB_SOAK_MAX_DATABASE_MIB": "128"}, true},
		{map[string]string{"DJOB_SOAK_DURATION": "75s", "DJOB_SOAK_HISTORY_WINDOW": "1m", "DJOB_SOAK_COMPACT_CONFIGURATION_RECEIPTS": "true"}, true},
		{map[string]string{"DJOB_SOAK_COMPACT_CONFIGURATION_RECEIPTS": "true"}, false},
		{map[string]string{"DJOB_SOAK_COMPACT_CONFIGURATION_RECEIPTS": "invalid"}, false},
		{map[string]string{"DJOB_SOAK_BATCH_SIZE": "0"}, false},
		{map[string]string{"DJOB_SOAK_DURATION": ""}, false},
		{map[string]string{"DJOB_SOAK_DURATION": "25h"}, false},
		{map[string]string{"DJOB_SOAK_MAX_LATENCY": "0s"}, false},
		{map[string]string{"DJOB_SOAK_FAULT_EVERY": "-1"}, false},
		{map[string]string{"DJOB_SOAK_MAX_RSS_MIB": "oops"}, false},
		{map[string]string{"DJOB_SOAK_MAX_OPEN_FILES": "-2"}, false},
		{map[string]string{"DJOB_SOAK_HISTORY_WINDOW": "-1m"}, false},
		{map[string]string{"DJOB_SOAK_HISTORY_WINDOW": "30s"}, false},
		{map[string]string{"DJOB_SOAK_HISTORY_WINDOW": "1m"}, false},
		{map[string]string{"DJOB_SOAK_GO_MEMORY_MIB": "8"}, false},
		{map[string]string{"DJOB_SOAK_CYCLE_INTERVAL": "2m"}, false},
		{map[string]string{"DJOB_SOAK_MAX_DATABASE_MIB": "-1"}, false},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.settings), func(t *testing.T) {
			_, err := loadSoakConfig(func(key string) (string, bool) { v, ok := c.settings[key]; return v, ok })
			if (err == nil) != c.valid {
				t.Fatalf("settings %v: %v", c.settings, err)
			}
		})
	}
}

// Use LookupEnv so explicitly empty or misspelled values fail instead of
// silently running a short smoke test in place of a requested qualification.
func soakConfiguration(t *testing.T) soakConfig {
	t.Helper()
	cfg, err := loadSoakConfig(os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
