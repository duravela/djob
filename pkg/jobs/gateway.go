package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
)

type GatewayBackend interface {
	GetChannel(string) (*db.CommunicationChannel, error)
	GetJobResource(string) (*db.JobResource, error)
}

// GatewayTarget is part of a driver binding, never application job input.
type GatewayTarget struct {
	ChannelID string `json:"channel_id"`
}
type OperationEncoder func(binding db.JobBinding, action Action) (executor string, payload []byte, err error)

// GatewayFactory bridges existing one-exchange transports (including Modbus TCP)
// into conversations. The encoder translates semantic operations to protocol
// payloads. Recovery/close runs after every exchange while the conversation's
// canonical gateway reservation remains held across all actions and waits.
func GatewayFactory(backend GatewayBackend, factory executor.Factory, encode OperationEncoder, recoveryTimeout time.Duration) DriverFactory {
	return func(run db.JobRun) (Driver, error) {
		if backend == nil || factory == nil || encode == nil || recoveryTimeout <= 0 || run.Binding == nil {
			return nil, fmt.Errorf("gateway driver requires a backend, transport, encoder, timeout, and binding")
		}
		var target GatewayTarget
		if err := json.Unmarshal(run.Binding.Config, &target); err != nil {
			return nil, err
		}
		if !slices.Contains(run.Resources, target.ChannelID) {
			return nil, fmt.Errorf("gateway target is not reserved by this run")
		}
		resource, err := backend.GetJobResource(target.ChannelID)
		if err != nil {
			return nil, err
		}
		if resource.Spec.ChannelID != target.ChannelID || resource.State != db.ResourceOwned || resource.TenantID != run.TenantID || resource.RunID != run.ID || resource.AttemptID != run.AttemptID || resource.OwnerID != run.OwnerID {
			return nil, fmt.Errorf("binding does not match the committed gateway resource")
		}
		channel, err := backend.GetChannel(target.ChannelID)
		if err != nil {
			return nil, err
		}
		if channel.OwnerID != run.OwnerID || channel.Epoch != resource.ChannelEpoch || channel.State != db.ChannelOwned {
			return nil, fmt.Errorf("gateway authority changed")
		}
		return &gatewayDriver{binding: *run.Binding, channel: *channel, factory: factory, encode: encode, timeout: recoveryTimeout}, nil
	}
}

type gatewayDriver struct {
	binding   db.JobBinding
	channel   db.CommunicationChannel
	factory   executor.Factory
	encode    OperationEncoder
	timeout   time.Duration
	transport executor.Transport
	unsafe    error
}

func (d *gatewayDriver) Execute(ctx context.Context, action Action) Effect {
	if d.unsafe != nil {
		return Effect{Status: db.EffectFailed, Error: d.unsafe.Error(), SessionLost: true}
	}
	executorName, payload, err := d.encode(d.binding, action)
	if err != nil {
		return Effect{Status: db.EffectFailed, Error: err.Error()}
	}
	d.transport, err = d.factory(d.channel)
	if err != nil || d.transport == nil {
		if err == nil {
			err = fmt.Errorf("gateway transport factory returned nil")
		}
		return Effect{Status: db.EffectFailed, Error: err.Error()}
	}
	outcome, exchangeErr := d.transport.Exchange(ctx, executor.Request{Channel: d.channel, RunID: action.RunID, AttemptID: action.AttemptID, Executor: executorName, Payload: payload})
	recovery, cancel := context.WithTimeout(context.Background(), d.timeout)
	recoverErr := d.transport.Recover(recovery)
	cancel()
	closing, cancel := context.WithTimeout(context.Background(), d.timeout)
	closeErr := d.transport.Close(closing)
	cancel()
	d.unsafe = errors.Join(recoverErr, closeErr)
	if d.unsafe == nil {
		d.transport = nil
	}
	status := db.EffectUnknown
	switch outcome.Status {
	case db.AttemptSucceeded:
		status = db.EffectSucceeded
	case db.AttemptFailed:
		status = db.EffectFailed
	}
	message := ""
	if err := errors.Join(exchangeErr, d.unsafe); err != nil {
		message = err.Error()
	}
	return Effect{Status: status, Data: outcome.Data, Error: message, SessionLost: d.unsafe != nil}
}

func (d *gatewayDriver) Quiesce(ctx context.Context) error {
	if d.transport != nil {
		return errors.Join(d.unsafe, d.transport.Recover(ctx))
	}
	return d.unsafe
}
func (d *gatewayDriver) Close(ctx context.Context) error {
	if d.transport != nil {
		return errors.Join(d.unsafe, d.transport.Close(ctx))
	}
	return d.unsafe
}
