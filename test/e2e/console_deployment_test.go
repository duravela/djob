package e2e_test

import (
	"os"
	"testing"
)

func TestConsoleDeploymentManifest(t *testing.T) {
	var kube *kubernetesFixture
	h := newHarness(t, false, func(h *harness) { h.remote = true; kube = startKubernetesFixture(h) })
	data, err := os.ReadFile("../../deploy/console/kubernetes.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.node.CopyToContainer(h.ctx, data, "/tmp/console-deployment.yaml", 0600); err != nil {
		t.Fatal(err)
	}
	kube.kubectl("apply", "--dry-run=server", "--validate=strict", "-f", "/tmp/console-deployment.yaml")
}
