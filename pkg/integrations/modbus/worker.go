// Package modbusdriver adapts the supplied Modbus transports to an external
// worker's controller-verified gateway allocation. It performs no Raft or local
// database operations. Existing embedded gateway integrations remain available.
package modbusdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/executor/modbustcp"
	"github.com/duravela/djob/pkg/jobs"
	modbusbinding "github.com/duravela/djob/pkg/jobs/bindings/modbus"
	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
)

const Version = "1"

// NewFactory enables one explicitly selected transport. Endpoint and ownership
// snapshots arrive only on an authorized action, never from application input.
func NewFactory(protocol string, recovery time.Duration) (djob.DriverFactory, error) {
	var transport executor.Factory
	switch protocol {
	case modbustcp.Protocol:
		transport = modbustcp.Factory
	case modbusrtutcp.Protocol:
		transport = modbusrtutcp.Factory
	default:
		return nil, fmt.Errorf("unsupported worker Modbus transport")
	}
	if recovery <= 0 {
		return nil, fmt.Errorf("Modbus recovery timeout must be positive")
	}
	return func(run djob.JobRun) (djob.Driver, error) {
		if run.Binding == nil {
			return nil, fmt.Errorf("worker Modbus driver requires a binding")
		}
		var target jobs.GatewayTarget
		if json.Unmarshal(run.Binding.Config, &target) != nil || target.ChannelID == "" || !slices.Contains(run.Resources, target.ChannelID) {
			return nil, fmt.Errorf("worker Modbus target must be a reserved canonical resource")
		}
		return &driver{run: run, target: target.ChannelID, protocol: protocol, transport: transport, recovery: recovery}, nil
	}, nil
}

type driver struct {
	run                         djob.JobRun
	target, protocol            string
	transport                   executor.Factory
	recovery                    time.Duration
	inner                       djob.Driver
	resourceEpoch, channelEpoch uint64
}

func (d *driver) Execute(ctx context.Context, action djob.Action) djob.Effect {
	fail := func() djob.Effect {
		return djob.Effect{Status: djob.EffectFailed, Error: "worker Modbus allocation does not match this attempt", SessionLost: true}
	}
	grants, valid := worker.Gateways(ctx)
	if !valid || action.TenantID != d.run.TenantID || action.RunID != d.run.ID || action.AttemptID != d.run.AttemptID {
		return fail()
	}
	var grant *wire.Gateway
	for i := range grants {
		if grants[i].ResourceID == d.target {
			if grant != nil {
				return fail()
			}
			grant = &grants[i]
		}
	}
	if grant == nil || grant.ChannelID != d.target || grant.GatewayID != d.target || grant.Protocol != d.protocol || grant.OwnerID != d.run.OwnerID || grant.ResourceEpoch == 0 || grant.Epoch == 0 || grant.Capacity != 1 || (grant.Duplex != "half" && grant.Duplex != "full") {
		return fail()
	}
	if d.inner != nil && (grant.ResourceEpoch != d.resourceEpoch || grant.Epoch != d.channelEpoch) {
		return fail()
	}
	if d.inner == nil {
		factory := jobs.GatewayFactory(allocation{run: d.run, grant: *grant}, d.transport, modbusbinding.Encode, d.recovery)
		var err error
		d.inner, err = factory(d.run)
		if err != nil {
			return djob.Effect{Status: djob.EffectFailed, Error: err.Error()}
		}
		d.resourceEpoch, d.channelEpoch = grant.ResourceEpoch, grant.Epoch
	}
	if ctx.Err() != nil {
		return fail()
	}
	return d.inner.Execute(ctx, action)
}
func (d *driver) Quiesce(ctx context.Context) error {
	if d.inner != nil {
		return d.inner.Quiesce(ctx)
	}
	return nil
}
func (d *driver) Close(ctx context.Context) error {
	if d.inner != nil {
		return d.inner.Close(ctx)
	}
	return nil
}

type allocation struct {
	run   djob.JobRun
	grant wire.Gateway
}

func (a allocation) GetChannel(id string) (*db.CommunicationChannel, error) {
	g := a.grant
	if id != g.ChannelID {
		return nil, fmt.Errorf("gateway is outside the worker allocation")
	}
	return &db.CommunicationChannel{ID: g.ChannelID, GatewayID: g.GatewayID, Protocol: g.Protocol, Endpoint: g.Endpoint, Revision: g.Revision, Epoch: g.Epoch, State: db.ChannelOwned, OwnerID: g.OwnerID, LeaseUntil: g.LeaseUntil, LastTransition: g.LastTransition}, nil
}
func (a allocation) GetJobResource(id string) (*db.JobResource, error) {
	g, r := a.grant, a.run
	if id != g.ResourceID {
		return nil, fmt.Errorf("resource is outside the worker allocation")
	}
	return &db.JobResource{Spec: db.ResourceSpec{ID: id, ChannelID: g.ChannelID, Capacity: g.Capacity, Duplex: g.Duplex}, Revision: g.ResourceRevision, Epoch: g.ResourceEpoch, State: db.ResourceOwned, TenantID: r.TenantID, RunID: r.ID, AttemptID: r.AttemptID, OwnerID: r.OwnerID, ChannelEpoch: g.Epoch, LeaseUntil: g.LeaseUntil}, nil
}
