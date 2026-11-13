package mqttdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/jobs"
)

func runSpec(kind string) db.JobRun {
	config, _ := json.Marshal(Binding{Connection: "lab", Operations: []Operation{{Name: "execute", Kind: kind, Topic: "requests/{request_id}", ReplyTopic: func() string {
		if kind == "request_reply" {
			return "replies/{request_id}"
		}
		return ""
	}(), RequireCorrelation: kind == "request_reply"}}})
	return db.JobRun{TenantID: "alpha", ID: "job", AttemptID: "attempt", Binding: &db.JobBinding{Config: config}}
}
func profiles() map[string]Profile {
	return map[string]Profile{"lab": {Broker: "tcp://127.0.0.1:1883", Tenants: []string{"alpha"}}}
}
func testDriver(t *testing.T, run db.JobRun, options Options) *driver {
	t.Helper()
	if options.OperationTimeout == 0 {
		options.OperationTimeout = time.Second
	}
	factory, err := NewFactory(profiles(), options)
	if err != nil {
		t.Fatal(err)
	}
	value, err := factory(run)
	if err != nil {
		t.Fatal(err)
	}
	return value.(*driver)
}
func action() jobs.Action {
	return jobs.Action{TenantID: "alpha", RunID: "job", AttemptID: "attempt", Sequence: 1, Operation: "execute", Input: []byte(`{"request":true}`), IdempotencyKey: "durable-key"}
}

type brokerPlan struct {
	replies    bool
	disconnect bool
	reject     bool
}

func fakeBroker(t *testing.T, d *driver, plan brokerPlan) <-chan *packets.Publish {
	t.Helper()
	client, broker := net.Pipe()
	received := make(chan *packets.Publish, 1)
	done := make(chan struct{})
	d.dial = func(context.Context, Profile) (net.Conn, error) { return client, nil }
	go func() {
		defer close(done)
		defer broker.Close()
		_ = broker.SetDeadline(time.Now().Add(5 * time.Second))
		read := func() *packets.ControlPacket {
			for {
				packet, err := packets.ReadPacket(broker)
				if err != nil {
					return nil
				}
				if _, ok := packet.Content.(*packets.Pingreq); ok {
					if _, err := (&packets.Pingresp{}).WriteTo(broker); err != nil {
						return nil
					}
					continue
				}
				return packet
			}
		}
		packet := read()
		if packet == nil {
			return
		}
		connect, ok := packet.Content.(*packets.Connect)
		if !ok || !connect.CleanStart || connect.ProtocolVersion != 5 {
			t.Error("client must start a clean MQTT 5 session")
			return
		}
		ack := &packets.Connack{Properties: &packets.Properties{}}
		if plan.reject {
			ack.ReasonCode = 0x87
			ack.Properties.ReasonString = "secret-marker-must-not-escape"
		}
		if _, err := ack.WriteTo(broker); err != nil || plan.reject {
			return
		}
		packet = read()
		if packet == nil {
			return
		}
		replyTopic := ""
		if sub, ok := packet.Content.(*packets.Subscribe); ok {
			replyTopic = sub.Subscriptions[0].Topic
			if len(sub.Subscriptions) != 1 || sub.Subscriptions[0].RetainHandling != 2 || !sub.Subscriptions[0].RetainAsPublished {
				t.Error("retained replay must be disabled")
				return
			}
			if _, err := (&packets.Suback{PacketID: sub.PacketID, Reasons: []byte{1}, Properties: &packets.Properties{}}).WriteTo(broker); err != nil {
				return
			}
			packet = read()
		}
		if packet == nil {
			return
		}
		pub, ok := packet.Content.(*packets.Publish)
		if !ok {
			t.Errorf("expected one publication, got %T", packet.Content)
			return
		}
		received <- pub
		if plan.disconnect {
			return
		}
		if pub.QoS != 1 || pub.Retain {
			t.Error("commands must use non-retained QoS 1")
		}
		if _, err := (&packets.Puback{PacketID: pub.PacketID, Properties: &packets.Properties{}}).WriteTo(broker); err != nil {
			return
		}
		if plan.replies {
			for _, reply := range []*packets.Publish{
				{Topic: "another/reply", Payload: []byte("foreign")},
				{Topic: replyTopic, Retain: true, Payload: []byte("retained")},
				{Topic: replyTopic, Properties: &packets.Properties{CorrelationData: []byte("another")}, Payload: []byte("stale")},
				{Topic: replyTopic, Properties: &packets.Properties{CorrelationData: pub.Properties.CorrelationData}, Payload: []byte("live")},
			} {
				if _, err := reply.WriteTo(broker); err != nil {
					return
				}
			}
		}
		// Do not cause an artificial disconnect before the driver consumes the reply.
		_, _ = io.Copy(io.Discard, broker)
	}()
	t.Cleanup(func() { _ = client.Close(); _ = broker.Close(); <-done })
	return received
}

func TestPublishAndCorrelatedReply(t *testing.T) {
	for _, kind := range []string{"publish", "request_reply"} {
		t.Run(kind, func(t *testing.T) {
			d := testDriver(t, runSpec(kind), Options{})
			messages := fakeBroker(t, d, brokerPlan{replies: kind == "request_reply"})
			effect := d.Execute(context.Background(), action())
			if effect.Status != db.EffectSucceeded || d.Quiesce(context.Background()) != nil {
				t.Fatalf("effect: %+v", effect)
			}
			if kind == "request_reply" && string(effect.Data) != "live" {
				t.Fatalf("accepted stale, foreign or retained reply: %q", effect.Data)
			}
			pub := <-messages
			if string(pub.Payload) != string(action().Input) || strings.Contains(pub.Topic, "{") {
				t.Fatalf("request mapping: %+v", pub)
			}
			if kind == "request_reply" && (pub.Properties.ResponseTopic == "" || len(pub.Properties.CorrelationData) != 48) {
				t.Fatal("missing fresh reply identity")
			}
		})
	}
}

func TestInterruptedPublicationStaysUnknown(t *testing.T) {
	d := testDriver(t, runSpec("publish"), Options{})
	fakeBroker(t, d, brokerPlan{disconnect: true})
	effect := d.Execute(context.Background(), action())
	if effect.Status != db.EffectUnknown || !effect.SessionLost || d.Quiesce(context.Background()) == nil {
		t.Fatalf("uncertain exchange: %+v", effect)
	}
	_ = d.Close(context.Background())
	if d.Quiesce(context.Background()) == nil {
		t.Fatal("closing a socket erased downstream uncertainty")
	}
}

func TestCancellationAfterBrokerAckDoesNotConfirmDeviceCompletion(t *testing.T) {
	d := testDriver(t, runSpec("request_reply"), Options{})
	requests := fakeBroker(t, d, brokerPlan{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan jobs.Effect, 1)
	go func() { done <- d.Execute(ctx, action()) }()
	select {
	case <-requests:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("client did not publish")
	}
	cancel()
	select {
	case effect := <-done:
		if effect.Status != db.EffectUnknown || d.Quiesce(context.Background()) == nil {
			t.Fatalf("cancelled action: %+v", effect)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MQTT execution did not stop after cancellation")
	}
}

func TestConnectionRejectionDoesNotEchoBrokerData(t *testing.T) {
	d := testDriver(t, runSpec("publish"), Options{})
	fakeBroker(t, d, brokerPlan{reject: true})
	effect := d.Execute(context.Background(), action())
	if effect.Status != db.EffectFailed || strings.Contains(effect.Error, "secret-marker") {
		t.Fatalf("connection rejection leaked data or became uncertain: %+v", effect)
	}
}

func TestResourceReplyRequiresDeviceCompletionVerifier(t *testing.T) {
	for _, accept := range []bool{false, true} {
		t.Run(fmt.Sprint(accept), func(t *testing.T) {
			run := runSpec("request_reply")
			run.Resources = []string{"physical-bus"}
			d := testDriver(t, run, Options{VerifyResourceCompletion: func(_ context.Context, frozen db.JobRun, _ jobs.Action, reply Reply) error {
				if !accept || string(reply.Payload) != "live" || len(frozen.Resources) != 1 {
					return fmt.Errorf("device is not quiescent")
				}
				return nil
			}})
			fakeBroker(t, d, brokerPlan{replies: true})
			effect := d.Execute(context.Background(), action())
			if accept && (effect.Status != db.EffectSucceeded || d.Quiesce(context.Background()) != nil) {
				t.Fatalf("verified reply: %+v", effect)
			}
			if !accept && (effect.Status != db.EffectUnknown || d.Quiesce(context.Background()) == nil) {
				t.Fatalf("unverified reply released resource: %+v", effect)
			}
		})
	}
}
