package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Two members cold-rebuild from a native 80 MiB-value checkpoint and its
// committed WAL tail. The third starts empty and must receive the large Raft
// snapshot. Every real controller has a 512 MiB cgroup cap with swap disabled.
func TestLargeSnapshotRecovery(t *testing.T) {
	report := object{"started_at": time.Now().UTC(), "raw_value_bytes": 80 << 20, "controller_go_memory_limit_mib": 256, "container_memory_limit_mib": 512, "max_database_mib": 192, "roles": []string{"cold-checkpoint-and-WAL", "cold-checkpoint-and-WAL", "empty-member-snapshot-transfer"}, "scope": "Bounded large-state cold restore and Raft snapshot transfer, not unlimited history or all replacement-state sizes. Kernel high-water RSS accounting can be imprecise; cgroup peak includes file cache and the inspection helper."}
	report["host_arch"], report["host_logical_cpus"], report["host_go_version"] = runtime.GOARCH, runtime.NumCPU(), runtime.Version()
	t.Cleanup(func() {
		report["passed"], report["finished_at"] = !t.Failed(), time.Now().UTC()
		if dir := os.Getenv("DJOB_TEST_ARTIFACT_DIR"); dir != "" {
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Error(err)
				return
			}
			data, err := json.MarshalIndent(report, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(dir, "large-state-recovery-report.json"), append(data, '\n'), 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}
	})
	var seeds []object
	h := newHarnessWithTimeout(t, false, 6*time.Minute, func(h *harness) {
		h.remote = true
		h.controllerMemoryLimit = 512 << 20
		h.controllerEnv = map[string]string{"GOMEMLIMIT": "256MiB", "GOMAXPROCS": "2"}
		h.controllerArgs = []string{"--max-database-bytes", fmt.Sprint(192 << 20)}
		for i := range h.volumes {
			h.volumes[i] = h.newVolume()
			if i < 2 {
				seeds = append(seeds, h.restoreFixture("seed", i))
				report["seeds"] = seeds
			}
		}
	})
	report["seeds"] = seeds
	code, identity, err := h.members[0].Exec(h.ctx, []string{"/djob", "version"}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		t.Fatalf("controller identity: %d %v", code, err)
	}
	identityBytes, err := io.ReadAll(identity)
	if err != nil {
		t.Fatal(err)
	}
	var build object
	if err := json.Unmarshal(identityBytes, &build); err != nil {
		t.Fatal(err)
	}
	report["binary"] = build
	if data, err := os.ReadFile(filepath.Join(os.Getenv("DJOB_E2E_BUILD_DIR"), "release-manifest.json")); err == nil {
		var manifest object
		if err := json.Unmarshal(data, &manifest); err != nil {
			t.Fatal(err)
		}
		report["release_manifest"] = manifest
	}
	var resources []object
	for i, member := range h.members {
		h.ready(i)
		// A quorum read after restoring the snapshot must include the committed
		// tail. The offline full-value audit below checks its durable effect.
		code, reader, err := member.Exec(h.ctx, []string{"/gateway", "--process-high-water"}, tcexec.Multiplexed())
		if err != nil || code != 0 {
			t.Fatalf("memory inspection: code=%d err=%v", code, err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		var memory object
		if err := json.Unmarshal(data, &memory); err != nil {
			t.Fatal(err)
		}
		if memory["peak_resident_bytes"].(float64) > 512<<20 || memory["cgroup_peak_bytes"].(float64) > 512<<20 || memory["cgroup_limit_bytes"] != float64(512<<20) || memory["oom_kills"] != float64(0) {
			t.Fatalf("controller %d memory limits: %v", i+1, memory)
		}
		info, err := member.Inspect(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.HostConfig.Memory != 512<<20 || info.HostConfig.MemorySwap != 512<<20 || info.State.OOMKilled {
			t.Fatal("container memory policy differs")
		}
		memory["member_id"] = i + 1
		resources = append(resources, memory)
	}
	report["resources"] = resources
	for i := range h.members {
		h.stopMember(i)
		state, err := h.members[i].State(h.ctx)
		if err != nil || state.OOMKilled {
			t.Fatalf("member %d failed during stop: %v", i+1, err)
		}
	}
	var verified []object
	for i := range h.members {
		verified = append(verified, h.restoreFixture("verify", i))
	}
	report["verified"] = verified
	for _, fixture := range append(seeds, verified...) {
		if fixture["source_revision"] != build["revision"] {
			t.Fatalf("fixture and controller revisions differ: %v/%v", fixture["source_revision"], build["revision"])
		}
	}
}

func (h *harness) restoreFixture(mode string, member int) object {
	h.t.Helper()
	c, err := testcontainers.Run(h.ctx, h.image,
		testcontainers.WithEntrypoint("/restore-fixture"),
		testcontainers.WithCmd(mode, "/data", fmt.Sprint(member+1)),
		testcontainers.WithEnv(map[string]string{"GOMEMLIMIT": "256MiB", "GOMAXPROCS": "2"}),
		testcontainers.WithMounts(testcontainers.VolumeMount(h.volumes[member], "/data")),
		testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(2*time.Minute)))
	if c != nil {
		defer c.Terminate(context.Background())
	}
	if err != nil {
		h.t.Fatal(err)
	}
	state, err := c.State(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	reader, err := c.Logs(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil || state.ExitCode != 0 {
		h.t.Fatalf("%s fixture exit=%d err=%v: %s", mode, state.ExitCode, err, data)
	}
	var report object
	if err := json.Unmarshal(data, &report); err != nil {
		h.t.Fatalf("fixture report: %v: %s", err, data)
	}
	if report["passed"] != true || report["raw_value_bytes"] != float64(80<<20) || report["checkpoint_index"] != float64(160) || report["wal_tail_index"] != float64(161) {
		h.t.Fatalf("fixture differs: %v", report)
	}
	if mode == "verify" && report["verified_jobs"] != float64(20481) {
		h.t.Fatalf("incomplete restored values: %v", report)
	}
	if mode == "verify" && (report["snapshot_index"] != float64(160) || report["snapshot_term"] != float64(1)) {
		h.t.Fatalf("initial checkpoint was not installed: %v", report)
	}
	return report
}
