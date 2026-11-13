package e2e_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

type mqttFixture struct {
	broker, diorama *testcontainers.DockerContainer
	control         gatewayControl
}

func (fixture *mqttFixture) prepare(h *harness) {
	t := h.t
	deadline, _ := h.ctx.Deadline()
	ca := newAuthority(t, time.Until(deadline)+time.Hour)
	cert, key := ca.leaf(t, "mqtt-broker")
	config := []byte("listener 1883\nallow_anonymous true\nlistener 8883\nallow_anonymous true\ncertfile /mosquitto/config/server.pem\nkeyfile /mosquitto/config/server-key.pem\npersistence false\n")
	broker, err := testcontainers.Run(h.ctx, "eclipse-mosquitto:2.1.2-alpine", testcontainers.WithExposedPorts("1883/tcp"), network.WithNetwork([]string{"mqtt-broker"}, h.network), testcontainers.WithFiles(
		testcontainers.ContainerFile{Reader: bytes.NewReader(config), ContainerFilePath: "/mosquitto/config/mosquitto.conf", FileMode: 0644},
		testcontainers.ContainerFile{Reader: bytes.NewReader(cert), ContainerFilePath: "/mosquitto/config/server.pem", FileMode: 0644},
		testcontainers.ContainerFile{Reader: bytes.NewReader(key), ContainerFilePath: "/mosquitto/config/server-key.pem", FileMode: 0644},
	), testcontainers.WithWaitStrategy(wait.ForListeningPort("1883/tcp").SkipInternalCheck()))
	fixture.broker = broker
	h.own(broker, "mqtt-broker")
	if err != nil {
		t.Fatal(err)
	}
	profiles, _ := json.Marshal(object{"connections": object{
		"lab":         object{"broker": "tls://mqtt-broker:8883", "ca_file": "/mqtt-ca.pem", "tenants": []string{"alpha", "beta"}},
		"alpha-only":  object{"broker": "tls://mqtt-broker:8883", "ca_file": "/mqtt-ca.pem", "tenants": []string{"alpha"}},
		"wrong-trust": object{"broker": "tls://mqtt-broker:8883", "ca_file": "/ca.pem", "tenants": []string{"alpha"}},
	}})
	h.mqttFiles = map[string][]byte{"/mqtt.json": profiles, "/mqtt-ca.pem": ca.pem}
	scenario, err := os.ReadFile("diorama.yaml")
	if err != nil {
		t.Fatal(err)
	}
	diorama, err := testcontainers.Run(h.ctx, h.image, testcontainers.WithEntrypoint("/diorama"), testcontainers.WithEnv(map[string]string{"DIORAMA_CONFIG_DIR": "/diorama.yaml", "DIORAMA_CONTROL_ADDR": ":8080"}), testcontainers.WithFiles(testcontainers.ContainerFile{Reader: bytes.NewReader(scenario), ContainerFilePath: "/diorama.yaml", FileMode: 0644}), testcontainers.WithExposedPorts("8080/tcp"), network.WithNetwork([]string{"diorama"}, h.network), testcontainers.WithWaitStrategy(wait.ForHTTP("/readyz").WithPort("8080/tcp").WithStartupTimeout(time.Minute)))
	fixture.diorama = diorama
	fixture.control.h = h
	h.own(diorama, "diorama")
	if err != nil {
		t.Fatal(err)
	}
	fixture.control.url, err = diorama.PortEndpoint(h.ctx, "8080/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}
}

func newMQTTHarness(t *testing.T, remote bool, timeout time.Duration, setup ...func(*harness)) (*harness, *mqttFixture) {
	fixture := &mqttFixture{}
	options := []func(*harness){func(h *harness) { h.remote = remote; fixture.prepare(h) }}
	options = append(options, setup...)
	return newHarnessWithTimeout(t, false, timeout, options...), fixture
}

func (h *harness) mqttBinding(tenant, id, connection, kind, topic, reply string) {
	h.t.Helper()
	op := object{"name": "execute", "kind": kind, "topic": topic}
	if reply != "" {
		op["reply_topic"] = reply
	}
	h.mutate(0, "platform", tenant, "put_binding", object{"binding": object{"id": id, "driver": "mqtt", "driver_version": "1", "config": encode(object{"connection": connection, "operations": []object{op}})}})
}
