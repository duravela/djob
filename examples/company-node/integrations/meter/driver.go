// Package meter adapts the company's semantic measure operation to Modbus.
// It owns protocol selection and result normalization, independently of job rules.
package meter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"example.com/acme/djob-node/jobs/measurement"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/modbus"
	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/executor/modbustcp"
	"github.com/duravela/djob/pkg/jobs"
	modbusbinding "github.com/duravela/djob/pkg/jobs/bindings/modbus"
	djob "github.com/duravela/djob/sdk/go"
)

const Name = "acme.meter"
const Version = "1"

// NewFactory maps an operator-owned binding to device I/O and normalizes its
// result. Another integration can provide the same Reading using HTTP or MQTT.
func NewFactory(backend jobs.GatewayBackend) djob.DriverFactory {
	factory := jobs.GatewayFactory(backend, func(channel db.CommunicationChannel) (executor.Transport, error) {
		switch channel.Protocol {
		case modbustcp.Protocol:
			return modbustcp.Factory(channel)
		case modbusrtutcp.Protocol:
			return modbusrtutcp.Factory(channel)
		default:
			return nil, fmt.Errorf("unsupported company gateway protocol")
		}
	}, modbusbinding.Encode, 3*time.Second)
	return func(run djob.JobRun) (djob.Driver, error) {
		driver, err := factory(run)
		if err != nil {
			return nil, err
		}
		return measurementDriver{Driver: driver}, nil
	}
}

type measurementDriver struct{ djob.Driver }

func (d measurementDriver) Execute(ctx context.Context, action djob.Action) djob.Effect {
	effect := d.Driver.Execute(ctx, action)
	if effect.Status != djob.EffectSucceeded {
		return effect
	}
	var result modbus.Result
	if json.Unmarshal(effect.Data, &result) != nil || len(result.Registers) != 1 {
		effect.Status, effect.Error = djob.EffectFailed, "measurement integration requires one register"
		effect.Data = nil
		return effect
	}
	effect.Data, _ = json.Marshal(measurement.Reading{Value: int(result.Registers[0])})
	return effect
}
