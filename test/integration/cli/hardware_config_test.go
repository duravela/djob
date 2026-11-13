package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func benchConfig(protocol, endpoint string) hardwareConfig {
	return hardwareConfig{
		Version: 1, Model: "loopback simulator", Firmware: "test fixture", PhysicalBus: "isolated test bus",
		Protocol: protocol, Endpoint: endpoint, SerialSettings: "9600 baud, 8 data bits, even parity, 1 stop bit",
		IsolatedBench: true, OtherMastersExcluded: true, Cycles: 1,
		Reads: []json.RawMessage{json.RawMessage(`{"version":1,"unit_id":3,"function":"read_holding_registers","address":20,"quantity":1}`)},
	}
}

func TestHardwareQualificationReportWithSimulator(t *testing.T) {
	if testing.Short() {
		t.Skip("three-process CLI test")
	}
	g := newGateway(t, true)
	config := filepath.Join(t.TempDir(), "approved.json")
	writeJSON(t, config, benchConfig("modbus-rtu-tcp", g.endpoint()))
	dir := filepath.Join(t.TempDir(), "private-run")
	t.Setenv("DJOB_HARDWARE_CONFIG", config)
	t.Setenv("DJOB_HARDWARE_RUN_DIR", dir)
	t.Setenv("DJOB_TEST_ARTIFACT_DIR", "")
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case call := <-g.calls:
				call.respond(t, false)
			case <-stop:
				return
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
	if !t.Run("run", TestHardwareGatewayQualification) {
		return
	}
	data, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report hardwareReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.AutomatedStatus != "passed" || report.PhysicalQualification != "pending" || report.FinishedAt == nil || report.FinishedAt.Before(report.StartedAt) || len(report.CLISHA256) != 64 || len(report.Checks) != 4 {
		t.Fatalf("invalid qualification report: %+v", report)
	}
	for path, mode := range map[string]os.FileMode{dir: 0700, filepath.Join(dir, "report.json"): 0600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != mode {
			t.Errorf("%s mode %o, want %o", path, info.Mode().Perm(), mode)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "node-1")); err != nil {
		t.Fatalf("member state was not retained: %v", err)
	}
	for _, path := range []string{"auth.json", "peer-tls/ca.pem", "peer-tls/member-1.pem", "peer-tls/member-1-key.pem", "member-1-start.json"} {
		info, err := os.Stat(filepath.Join(dir, path))
		if err != nil {
			t.Fatalf("retained credential/startup file %s: %v", path, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("retained credential/startup file %s is not private", path)
		}
	}
	g.check(t, 5)
}

func TestHardwareConfigValidation(t *testing.T) {
	valid := benchConfig("modbus-rtu-tcp", "tcp://192.0.2.1:1502?baud=9600&turnaround=5ms")
	check := func(t *testing.T, cfg hardwareConfig, valid bool) {
		t.Helper()
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeHardwareConfig(data)
		if (err == nil) != valid {
			t.Fatalf("valid=%v, decode error=%v", valid, err)
		}
	}
	check(t, valid, true)
	for _, function := range []string{"read_coils", "read_discrete_inputs", "read_holding_registers", "read_input_registers"} {
		t.Run(function, func(t *testing.T) {
			cfg := valid
			raw, _ := json.Marshal(object{"version": 1, "unit_id": 3, "function": function, "address": 20, "quantity": 1})
			cfg.Reads = []json.RawMessage{raw}
			check(t, cfg, true)
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*hardwareConfig)
	}{
		{"not isolated", func(c *hardwareConfig) { c.IsolatedBench = false }},
		{"old masters still connected", func(c *hardwareConfig) { c.OtherMastersExcluded = false }},
		{"no inventory", func(c *hardwareConfig) { c.PhysicalBus = "" }},
		{"missing firmware", func(c *hardwareConfig) { c.Firmware = "" }},
		{"no serial inventory", func(c *hardwareConfig) { c.SerialSettings = "" }},
		{"no reads", func(c *hardwareConfig) { c.Reads = nil }},
		{"no cycles", func(c *hardwareConfig) { c.Cycles = 0 }},
		{"too many cycles", func(c *hardwareConfig) { c.Cycles = 6 }},
		{"write", func(c *hardwareConfig) {
			c.Reads = []json.RawMessage{json.RawMessage(`{"version":1,"unit_id":3,"function":"write_single_register","address":20,"registers":[1]}`)}
		}},
		{"ambiguous operation", func(c *hardwareConfig) {
			c.Reads = []json.RawMessage{json.RawMessage(`{"version":1,"unit_id":3,"function":"write_single_register","function":"read_holding_registers","address":20,"quantity":1}`)}
		}},
		{"broadcast", func(c *hardwareConfig) {
			c.Reads = []json.RawMessage{json.RawMessage(`{"version":1,"unit_id":0,"function":"read_holding_registers","address":20,"quantity":1}`)}
		}},
		{"unsupported protocol", func(c *hardwareConfig) { c.Protocol = "serial" }},
		{"short turnaround", func(c *hardwareConfig) { c.Endpoint = "tcp://192.0.2.1:1502?baud=9600&turnaround=1ms" }},
		{"endpoint credentials", func(c *hardwareConfig) { c.Endpoint = "tcp://user:password@192.0.2.1:1502?baud=9600&turnaround=5ms" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.change(&cfg)
			check(t, cfg, false)
		})
	}
	t.Run("Modbus TCP", func(t *testing.T) {
		check(t, benchConfig("modbus-tcp", "tcp://192.0.2.1:1502?turnaround=5ms"), true)
	})
	for _, data := range []string{`{}`, `null`, `{"unknown":true}`, `{} {}`} {
		if _, err := decodeHardwareConfig([]byte(data)); err == nil {
			t.Errorf("accepted invalid config %s", data)
		}
	}
}

// Exercise the hardware procedure through real CLI members in normal CI. The
// simulator proves the harness and TCP-level exclusion, never physical timing.
func TestHardwareQualificationHarnessWithSimulator(t *testing.T) {
	for _, rtu := range []bool{false, true} {
		protocol := "modbus-tcp"
		if rtu {
			protocol = "modbus-rtu-tcp"
		}
		t.Run(protocol, func(t *testing.T) {
			g := newGateway(t, rtu)
			c := newCluster(t)
			cfg := benchConfig(protocol, g.endpoint())
			cfg.Reads = append(cfg.Reads, json.RawMessage(`{"version":1,"unit_id":7,"function":"read_holding_registers","address":21,"quantity":2}`))
			stop, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				for {
					select {
					case call := <-g.calls:
						call.respond(t, false)
					case <-stop:
						return
					}
				}
			}()
			t.Cleanup(func() { close(stop); <-done })
			report := hardwareReport{PhysicalQualification: "pending"}
			qualifyGateway(t, c, cfg, &report)
			g.check(t, 7) // four contenders, conversation, legacy and idle failover
			if len(report.Checks) != 4 || report.PhysicalQualification != "pending" {
				t.Fatalf("simulator changed the physical qualification claim: %+v", report)
			}
		})
	}
}
