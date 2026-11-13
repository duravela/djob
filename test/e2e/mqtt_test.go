package e2e_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestMQTTWithDiorama(t *testing.T) {
	testMQTTWithDiorama(t, false)
}

func TestRemoteMQTTWorkerWithDiorama(t *testing.T) {
	testMQTTWithDiorama(t, true)
}

func testMQTTWithDiorama(t *testing.T, remote bool) {
	h, fixture := newMQTTHarness(t, remote, 5*time.Minute)

	if remote {
		for i, tenant := range []string{"alpha", "beta"} {
			h.startWorker(tenant, "mqtt-"+tenant, "/djob-worker", i, "--mqtt-profiles", "/mqtt.json")
		}
	}
	for _, tenant := range []string{"alpha", "beta"} {
		h.mutate(0, "platform", tenant, "put_tenant", object{"max_running": "2"})
	}

	requestTopic, replyTopic := "gateway/gw-alpha-001/requests/devices/{request_id}", "gateway/gw-alpha-001/responses/devices/{request_id}"
	for i, tenant := range []string{"alpha", "beta"} {
		h.mqttBinding(tenant, "list", "lab", "request_reply", requestTopic, replyTopic)
		h.definition(tenant, "list", "invoke", "list", object{"operation": "list", "resource": "devices"})
		h.launch(i, tenant, "list", "devices")
		run := h.run(i, tenant, "devices", "succeeded")
		data, err := base64.StdEncoding.DecodeString(run["result"].(string))
		var result struct {
			Status string   `json:"status"`
			Items  []object `json:"items"`
		}
		if err != nil || json.Unmarshal(data, &result) != nil || result.Status != "ok" || len(result.Items) != 1 || result.Items[0]["serialNumber"] != "10000001" {
			t.Fatalf("unexpected Diorama device response: %s, %v", data, err)
		}
	}
	// A process-local profile cannot be selected by another tenant's binding.
	h.mqttBinding("beta", "forbidden", "alpha-only", "publish", "should/not/publish", "")
	h.definition("beta", "forbidden", "invoke", "forbidden", object{"command": true})
	h.launch(1, "beta", "forbidden", "forbidden")
	h.run(0, "beta", "forbidden", "failed")
	// TLS trust failure is known before publication and cannot become a success.
	h.mqttBinding("alpha", "wrong-trust", "wrong-trust", "publish", "should/not/publish", "")
	h.definition("alpha", "wrong-trust", "invoke", "wrong-trust", object{"command": true})
	h.launch(1, "alpha", "wrong-trust", "wrong-trust")
	h.run(0, "alpha", "wrong-trust", "failed")
	// The broker acknowledges this publication, but nobody replies. Acceptance
	// alone cannot complete a request/reply action or authorize replay.
	h.mqttBinding("alpha", "no-reply", "lab", "request_reply", "unhandled/{request_id}", "replies/{request_id}")
	h.definition("alpha", "no-reply", "invoke", "no-reply", object{"request": true})
	h.launch(1, "alpha", "no-reply", "no-reply")
	h.run(0, "alpha", "no-reply", "unknown")
	h.mqttBinding("alpha", "publish", "lab", "publish", "gateway/gw-alpha-001/commands/reboot", "")
	h.definition("alpha", "publish", "invoke", "publish", object{"command": "reboot"})
	h.launch(2, "alpha", "publish", "publish")
	run := h.run(0, "alpha", "publish", "succeeded")
	data, _ := base64.StdEncoding.DecodeString(run["result"].(string))
	if string(data) != `{"broker_acknowledged":true}` {
		t.Fatalf("publication was misrepresented as device completion: %s", data)
	}
	// Diorama's state independently verifies that the published command arrived.
	poll(t, "Diorama observes its MQTT reboot command", func() (bool, error) {
		response, err := h.client.Get(fixture.control.url + "/sim/gateways/gw-alpha-001")
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		var result object
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil {
			return false, fmt.Errorf("invalid Diorama state")
		}
		encoded, _ := json.Marshal(result)
		return bytes.Contains(encoded, []byte(`rebooting`)), nil
	})
}
