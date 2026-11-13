package mqttdriver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/eclipse/paho.golang/paho"
	djob "github.com/duravela/djob/sdk/go"
)

type driver struct {
	profile    Profile
	operations map[string]Operation
	options    Options
	run        djob.JobRun
	dial       func(context.Context, Profile) (net.Conn, error)
	uncertain  bool
	closed     bool
}

func (d *driver) Execute(parent context.Context, action djob.Action) (effect djob.Effect) {
	failed := func(message string) djob.Effect { return djob.Effect{Status: djob.EffectFailed, Error: message} }
	if d.closed || d.uncertain {
		return failed("MQTT session is unavailable")
	}
	op, exists := d.operations[action.Operation]
	if !exists || len(action.Input) > maxPayload || action.TenantID != d.run.TenantID || action.RunID != d.run.ID || action.AttemptID != d.run.AttemptID || action.IdempotencyKey == "" || len(action.IdempotencyKey) > 256 {
		return failed("MQTT action does not match its binding, run or payload limit")
	}
	ctx, cancel := context.WithTimeout(parent, d.options.OperationTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return failed("MQTT action cancelled before connection")
	}
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return failed("MQTT request identity unavailable")
	}
	requestID := hex.EncodeToString(nonce[:])
	topic := strings.ReplaceAll(op.Topic, "{request_id}", requestID)
	replyTopic := strings.ReplaceAll(op.ReplyTopic, "{request_id}", requestID)
	if !validTopic(topic) || len(replyTopic) > 65535 {
		return failed("MQTT expanded topic exceeds its limit")
	}
	conn, err := d.dial(ctx, d.profile)
	if err != nil {
		return failed("MQTT connection failed before publication")
	}
	var closeOnce sync.Once
	closeConnection := func() {
		closeOnce.Do(func() {
			if secure, ok := conn.(*tls.Conn); ok {
				_ = secure.NetConn().Close()
			} else {
				_ = conn.Close()
			}
		})
	}
	stop := context.AfterFunc(ctx, closeConnection)
	connected := false
	var client *paho.Client
	defer func() {
		stop()
		closeConnection()
		// Execute never returns with a background client still able to send.
		// This is a clean, attempt-local session with no reconnect/replay loop.
		if connected {
			<-client.Done()
		}
	}()
	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return failed("MQTT connection deadline could not be established")
	}
	var started atomic.Bool
	replies := make(chan Reply, 1)
	invalid := make(chan struct{}, 1)
	client = paho.NewClient(paho.ClientConfig{
		Conn: &packetConn{Conn: conn}, PacketTimeout: d.options.OperationTimeout,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(received paho.PublishReceived) (bool, error) {
			message := received.Packet
			if !started.Load() || message.Topic != replyTopic || message.Retain || (op.RequireCorrelation && (message.Properties == nil || !bytes.Equal(message.Properties.CorrelationData, []byte(requestID)))) {
				return true, nil
			}
			if len(message.Payload) > maxPayload {
				select {
				case invalid <- struct{}{}:
				default:
				}
				return true, nil
			}
			select {
			case replies <- Reply{Topic: message.Topic, Payload: bytes.Clone(message.Payload)}:
			default:
			}
			return true, nil
		}},
	})
	maximum, receive := uint32(maxPacket), uint16(1)
	ack, err := client.Connect(ctx, &paho.Connect{ClientID: "djob-" + requestID, CleanStart: true, KeepAlive: 30, Username: d.profile.Username, UsernameFlag: d.profile.Username != "", Password: []byte(d.profile.Password), PasswordFlag: d.profile.Password != "", Properties: &paho.ConnectProperties{MaximumPacketSize: &maximum, ReceiveMaximum: &receive}})
	if err != nil {
		return failed("MQTT broker connection rejected or interrupted")
	}
	connected = true
	if ack.ReasonCode != 0 || ack.SessionPresent || (ack.Properties != nil && ack.Properties.MaximumQoS != nil && *ack.Properties.MaximumQoS < 1) {
		return failed("MQTT broker does not support the required clean QoS 1 session")
	}
	if op.Kind == "request_reply" {
		sub, err := client.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: replyTopic, QoS: 1, NoLocal: true, RetainHandling: 2, RetainAsPublished: true}}})
		if err != nil || sub == nil || len(sub.Reasons) != 1 || sub.Reasons[0] > 1 {
			return failed("MQTT reply subscription was not confirmed")
		}
	}
	if ctx.Err() != nil {
		return failed("MQTT action cancelled before publication")
	}
	properties := &paho.PublishProperties{User: paho.UserProperties{{Key: "djob-idempotency-key", Value: action.IdempotencyKey}}}
	if op.Kind == "request_reply" {
		properties.ResponseTopic, properties.CorrelationData = replyTopic, []byte(requestID)
	}
	d.uncertain = true // From this boundary onwards delivery is possible.
	started.Store(true)
	acknowledgement, err := client.Publish(ctx, &paho.Publish{QoS: 1, Topic: topic, Payload: action.Input, Properties: properties})
	unknown := func(message string) djob.Effect {
		return djob.Effect{Status: djob.EffectUnknown, Error: message, SessionLost: true}
	}
	if err != nil || acknowledgement == nil || acknowledgement.ReasonCode >= 128 {
		return unknown("MQTT publication outcome is uncertain")
	}
	if op.Kind == "publish" {
		d.uncertain = false
		data, _ := json.Marshal(struct {
			BrokerAcknowledged bool `json:"broker_acknowledged"`
		}{true})
		return djob.Effect{Status: djob.EffectSucceeded, Data: data}
	}
	select {
	case reply := <-replies:
		if len(d.run.Resources) != 0 && d.options.VerifyResourceCompletion(ctx, d.run, action, reply) != nil {
			return unknown("MQTT response did not establish downstream completion")
		}
		d.uncertain = false
		return djob.Effect{Status: djob.EffectSucceeded, Data: reply.Payload}
	case <-invalid:
		return unknown("MQTT response exceeds 1 MiB")
	case <-client.Done():
		return unknown("MQTT connection ended before a matching response")
	case <-ctx.Done():
		return unknown("MQTT response was not confirmed before the action deadline")
	}
}

func (d *driver) Quiesce(context.Context) error {
	if d.uncertain {
		return fmt.Errorf("MQTT downstream completion is uncertain")
	}
	return nil
}
func (d *driver) Close(context.Context) error { d.closed = true; return nil }
