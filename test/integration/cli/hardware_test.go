package cli_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/modbus"
	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/executor/modbustcp"
)

type hardwareConfig struct {
	Version              int               `json:"version"`
	Model                string            `json:"model"`
	Firmware             string            `json:"firmware"`
	PhysicalBus          string            `json:"physical_bus"`
	Protocol             string            `json:"protocol"`
	Endpoint             string            `json:"endpoint"`
	SerialSettings       string            `json:"serial_settings"`
	IsolatedBench        bool              `json:"isolated_bench"`
	OtherMastersExcluded bool              `json:"other_masters_excluded"`
	Cycles               int               `json:"cycles"`
	Reads                []json.RawMessage `json:"reads"`
}

func decodeHardwareConfig(data []byte) (hardwareConfig, error) {
	var cfg hardwareConfig
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&cfg); err != nil {
		return cfg, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return cfg, fmt.Errorf("hardware config must contain one JSON object")
	}
	if cfg.Version != 1 || !cfg.IsolatedBench || !cfg.OtherMastersExcluded {
		return cfg, fmt.Errorf("version 1, isolated_bench and other_masters_excluded confirmations are required")
	}
	if strings.TrimSpace(cfg.Model) == "" || strings.TrimSpace(cfg.Firmware) == "" || strings.TrimSpace(cfg.PhysicalBus) == "" {
		return cfg, fmt.Errorf("model, firmware and physical_bus inventory are required")
	}
	if cfg.Cycles < 1 || cfg.Cycles > 5 || len(cfg.Reads) < 1 || len(cfg.Reads) > 4 || cfg.Cycles*len(cfg.Reads) > 10 {
		return cfg, fmt.Errorf("use 1..5 cycles, 1..4 approved reads and at most 10 reads per tenant")
	}
	for _, raw := range cfg.Reads {
		op, err := modbus.DecodeOperation(raw)
		if err != nil {
			return cfg, err
		}
		switch op.Function {
		case "read_coils", "read_discrete_inputs", "read_holding_registers", "read_input_registers":
		default:
			return cfg, fmt.Errorf("hardware qualification permits approved reads only")
		}
	}
	// The production factories validate framing, endpoint and turnaround without
	// connecting. Reject invalid configuration before starting any CLI process.
	channel := db.CommunicationChannel{ID: "qualification-bus", GatewayID: "qualification-bus", Protocol: cfg.Protocol, Endpoint: cfg.Endpoint}
	var factory executor.Factory
	switch cfg.Protocol {
	case modbustcp.Protocol:
		factory = modbustcp.Factory
	case modbusrtutcp.Protocol:
		if strings.TrimSpace(cfg.SerialSettings) == "" {
			return cfg, fmt.Errorf("RTU serial_settings must record baud, parity, data and stop bits")
		}
		factory = modbusrtutcp.Factory
	default:
		return cfg, fmt.Errorf("unsupported hardware protocol %q", cfg.Protocol)
	}
	_, err := factory(channel)
	return cfg, err
}

type hardwareReport struct {
	Version               int            `json:"version"`
	Config                hardwareConfig `json:"config"`
	CLISHA256             string         `json:"cli_sha256"`
	TestGoVersion         string         `json:"test_go_version"`
	StartedAt             time.Time      `json:"started_at"`
	FinishedAt            *time.Time     `json:"finished_at,omitempty"`
	AutomatedStatus       string         `json:"automated_status"`
	PhysicalQualification string         `json:"physical_qualification"`
	Checks                []string       `json:"checks"`
}

// This is deliberately opt-in. No default endpoint, discovery, writes,
// automatic unblock or physical quiescence attestation is provided.
func TestHardwareGatewayQualification(t *testing.T) {
	configPath := os.Getenv("DJOB_HARDWARE_CONFIG")
	if configPath == "" || testing.Short() {
		t.Skip("set DJOB_HARDWARE_CONFIG and DJOB_HARDWARE_RUN_DIR for isolated hardware qualification")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := decodeHardwareConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("DJOB_TEST_ARTIFACT_DIR") != "" {
		t.Fatal("unset DJOB_TEST_ARTIFACT_DIR; hardware logs and state must stay in the private run directory")
	}
	dir := os.Getenv("DJOB_HARDWARE_RUN_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("DJOB_HARDWARE_RUN_DIR must be a new absolute private directory")
	}
	// Refuse reuse: old member identities and any uncertain effects must be
	// investigated rather than silently discarded by a new qualification run.
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	binary := cliBinary(t)
	f, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, err = io.Copy(hash, f)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("hash CLI: %v, close: %v", err, closeErr)
	}
	report := hardwareReport{Version: 1, Config: cfg, CLISHA256: hex.EncodeToString(hash.Sum(nil)), TestGoVersion: runtime.Version(), StartedAt: time.Now().UTC(), AutomatedStatus: "incomplete", PhysicalQualification: "pending", Checks: []string{}}
	reportFile, err := os.OpenFile(filepath.Join(dir, "report.json"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	saveReport := func() {
		t.Helper()
		if _, err := reportFile.Seek(0, io.SeekStart); err != nil {
			t.Error(err)
			return
		}
		if err := reportFile.Truncate(0); err != nil {
			t.Error(err)
			return
		}
		if err := json.NewEncoder(reportFile).Encode(report); err != nil {
			t.Error(err)
		}
		if err := reportFile.Sync(); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() {
		finished := time.Now().UTC()
		report.FinishedAt = &finished
		report.AutomatedStatus = "passed"
		if t.Failed() {
			report.AutomatedStatus = "failed"
		}
		saveReport()
		if err := reportFile.Close(); err != nil {
			t.Error(err)
		}
	})
	saveReport()
	if t.Failed() {
		t.FailNow()
	}
	c := newClusterInDir(t, binary, dir)
	c.pollTimeout = 5 * time.Minute // Several slow, serialized reads can precede a contender.
	qualifyGateway(t, c, cfg, &report)
}

func qualifyGateway(t *testing.T, c *cluster, cfg hardwareConfig, report *hardwareReport) {
	t.Helper()
	const bus = "qualification-bus"
	c.channel(0, bus, cfg.Protocol, cfg.Endpoint)
	c.mutate(0, "platform", "", "put_resource", object{"spec": object{"id": bus, "channel_id": bus, "duplex": "half", "capacity": 1}})
	for i, tenant := range []string{"alpha", "beta"} {
		c.mutate(i, "platform", tenant, "put_tenant", object{"max_running": "2"})
		c.mutate(i, "platform", tenant, "put_grant", object{"resource_id": bus, "allowed": true})
		for j, raw := range cfg.Reads {
			binding := fmt.Sprintf("read-%d", j)
			c.mutate(i, "platform", tenant, "put_binding", object{"binding": object{"id": binding, "driver": cfg.Protocol, "driver_version": "1", "resources": []string{bus}, "config": encoded(object{"channel_id": bus, "operations": []object{{"name": "execute", "operation": raw}, {"name": "request", "operation": raw}}})}})
			c.definition(i, tenant, binding, "invoke", binding, nil)
		}
	}
	c.definition(0, "alpha", "conversation", "request-reply", "read-0", object{"correlation_key": "qualification-reply", "timeout_ns": 120000000000})
	c.launch(0, "alpha", "conversation", "conversation")
	c.awaitRun(0, "alpha", "conversation", "waiting")
	report.Checks = append(report.Checks, "approved read followed by a resource-holding conversation wait")

	type contender struct{ tenant, id string }
	var contenders []contender
	for cycle := range cfg.Cycles {
		for j := range cfg.Reads {
			for i, tenant := range []string{"alpha", "beta"} {
				id := fmt.Sprintf("read-%d-%d", cycle, j)
				c.launch((cycle+i+j)%3, tenant, fmt.Sprintf("read-%d", j), id)
				contenders = append(contenders, contender{tenant, id})
			}
		}
	}
	var legacy object
	if err := json.Unmarshal(cfg.Reads[0], &legacy); err != nil {
		t.Fatal(err)
	}
	c.createLegacy(2, "legacy-read", bus, legacy)
	for _, v := range contenders {
		c.awaitRun(0, v.tenant, v.id, "ready")
	}
	c.awaitLegacy(0, "legacy-read", "ready")
	report.Checks = append(report.Checks, "two tenants and legacy dispatch remain queued while the conversation holds the shared bus")
	c.mutate(1, "alpha-callback", "", "signal", object{"run_id": "conversation", "event_id": "qualification-reply", "key": "qualification-reply"})
	c.awaitRun(0, "alpha", "conversation", "succeeded")
	for _, v := range contenders {
		c.awaitRun(0, v.tenant, v.id, "succeeded")
	}
	c.awaitLegacy(0, "legacy-read", "succeeded")
	c.awaitChannel(0, bus, "available")
	report.Checks = append(report.Checks, "approved reads complete across both tenants and legacy dispatch after release")

	// Fault the control plane only after the physical exchange has drained.
	// In-flight faults require external observation and fencing, done manually.
	leader := c.leader(-1)
	c.stop(leader, true)
	replacement := c.leader(leader)
	c.ready(replacement)
	c.launch(replacement, "alpha", "read-0", "after-idle-leader-loss")
	c.awaitRun(replacement, "alpha", "after-idle-leader-loss", "succeeded")
	c.awaitChannel(replacement, bus, "available")
	c.start(leader)
	c.waitStarted(leader)
	c.ready(leader)
	c.awaitChannel(leader, bus, "available")
	report.Checks = append(report.Checks, "an approved read succeeds after idle leader loss and the restarted member catches up")
}
