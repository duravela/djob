package e2e_test

import (
	"fmt"
	"testing"
)

func TestFailedRemoteClosePreservesQueuedWorkAndBlocksUncertainGateway(t *testing.T) {
	h := newHarness(t, false, func(h *harness) { h.remote = true })
	for _, tenant := range []string{"alpha", "beta"} {
		h.mutate(0, "platform", tenant, "put_tenant", object{"max_running": "4"})
	}
	endpoint, gateway := h.gateway(true)
	h.configure("bus", "modbus-rtu-tcp", endpoint, "acme.meter", "measure")
	h.definition("alpha", "conversation", "acme.measure-twice", "bus", object{"maximum": 100})
	h.definition("alpha", "compute", "sha256", "", object{"independent": true})
	h.definition("beta", "contender", "acme.check-measurement", "bus", object{"maximum": 100})
	broken := h.startWorkerControllers("alpha", "bad-close-alpha", "/company-worker", []int{0, 1, 2})
	h.launch(0, "alpha", "conversation", "bad-close")
	h.run(0, "alpha", "bad-close", "waiting")
	// The live conversation occupies this worker while independent work queues.
	h.launch(0, "alpha", "compute", "next-job")
	h.run(0, "alpha", "next-job", "ready")
	gateway.call("POST", "/corrupt-next")
	h.mutate(1, "alpha-callback", "", "signal", object{"run_id": "bad-close", "event_id": "continue", "key": "continue"})
	h.run(0, "alpha", "bad-close", "unknown")
	h.channel(0, "bus", "blocked")
	// A corrupt RTU exchange leaves the supplied driver's session unsafe. Its
	// Close reports that uncertainty, so Serve must stop with an error.
	poll(t, "worker exits after failed driver close", func() (bool, error) {
		info, err := broken.Inspect(h.ctx)
		if err != nil {
			return false, err
		}
		if info.State.Running {
			return false, fmt.Errorf("worker still running")
		}
		if info.State.ExitCode == 0 {
			t.Fatal("reported close failure exited successfully")
		}
		return true, nil
	})
	queued := h.run(0, "alpha", "next-job", "ready")
	if queued["attempt_count"] != "0" {
		t.Fatalf("retiring worker claimed queued work: %v", queued)
	}
	for sequence, want := range []string{"succeeded", "unknown"} {
		record := h.request(0, "POST", "/api/jobs/v1/get", "alpha-reader", object{"reference": object{"kind": "ACTION", "run_id": "bad-close", "sequence": fmt.Sprint(sequence + 1)}}, 200)
		action, _ := record["action"].(map[string]any)
		if action["status"] != want {
			t.Fatalf("cleanup changed action history: %v", record)
		}
	}
	for _, name := range []string{"djob_worker_sessions", "djob_worker_session_slots", "djob_worker_sessions_closing"} {
		poll(t, "failed worker releases controller capacity", func() (bool, error) { value, err := h.metric(0, name); return value == 0, err })
	}
	h.startWorker("alpha", "healthy-alpha", "/company-worker", 2)
	h.startWorker("beta", "contending-beta", "/company-worker", 1)
	h.run(0, "alpha", "next-job", "succeeded")
	h.launch(1, "beta", "contender", "blocked-contender")
	h.run(0, "beta", "blocked-contender", "ready")
	h.channel(0, "bus", "blocked")
	if stats := gateway.call("GET", "/stats"); stats["requests"] != float64(2) || stats["maximum"] != float64(1) {
		t.Fatalf("failed cleanup replayed or overlapped gateway I/O: %v", stats)
	}
}
