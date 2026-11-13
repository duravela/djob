package cli_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCLIRollingUpgradeFromPreviousBinary(t *testing.T) {
	previous := os.Getenv("DJOB_TEST_PREVIOUS_BINARY")
	if previous == "" {
		t.Skip("set DJOB_TEST_PREVIOUS_BINARY to exercise the previous deployed release")
	}
	absolute, err := filepath.Abs(previous)
	if err != nil {
		t.Fatal(err)
	}
	updated := cliBinary(t)
	c := newClusterWithBinary(t, absolute)
	c.mutate(0, "platform", "alpha", "put_tenant", object{"max_running": "2"})
	original := object{"command": c.meta(), "put_definition": object{"definition": object{"id": "hash", "runtime": "steps/v1", "handler": "sha256", "handler_version": "1", "input": encoded([]byte("rolling upgrade"))}}}
	expected := c.request(0, "POST", "/api/jobs/v1/mutate", "alpha-writer", original, 200)
	check := func(i int) {
		t.Helper()
		if result := c.request(i, "POST", "/api/jobs/v1/mutate", "alpha-writer", original, 200); !reflect.DeepEqual(result, expected) {
			t.Fatal("rolling upgrade changed an exact retry acknowledgement")
		}
	}
	// Upgrade two followers before the leader. The first new member cannot
	// establish clock agreement with the previous binaries' absent endpoint;
	// the previous quorum continues serving while the next follower upgrades.
	leader := c.leader(-1)
	var order []int
	for i := range c.members {
		if i != leader {
			order = append(order, i)
		}
	}
	order = append(order, leader)
	c.binary = updated
	for step, i := range order {
		c.stop(i, false)
		c.start(i)
		c.waitStarted(i)
		if step == 0 {
			check(leader)
			continue
		}
		for _, upgraded := range order[:step+1] {
			c.ready(upgraded)
			check(upgraded)
		}
	}
	for i := range c.members {
		c.ready(i)
	}
	c.launch(0, "alpha", "hash", "after-upgrade")
	c.awaitRun(0, "alpha", "after-upgrade", "succeeded")
}
