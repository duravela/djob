package main

import (
	"context"
	"strings"
	"testing"
)

func TestWorkerCommandRequiresPrivateConnectionConfiguration(t *testing.T) {
	for _, args := range [][]string{nil, {"--data-dir", "/tmp/unused"}, {"--worker-config", "missing", "extra"}, {"--recovery-timeout", "-1s"}} {
		if err := serve(context.Background(), args); err == nil {
			t.Fatalf("invalid worker flags accepted: %v", args)
		}
	}
	if err := serve(context.Background(), []string{"--worker-config", "/missing/secret-marker"}); err == nil || strings.Contains(err.Error(), "secret-marker") {
		t.Fatalf("connection configuration error leaked a path: %v", err)
	}
}
