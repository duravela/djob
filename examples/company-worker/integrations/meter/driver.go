// Package meter maps the semantic measure operation onto the company's chosen
// protocol. A different integration can return the same domain Reading.
package meter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"example.com/acme/djob-worker/jobs/measurement"
	"github.com/duravela/djob/pkg/executor/modbus"
	modbusdriver "github.com/duravela/djob/pkg/integrations/modbus"
	"github.com/duravela/djob/sdk/go"
)

const Name = "acme.meter"
const Version = "1"

func NewFactory(recovery time.Duration) djob.DriverFactory {
	return func(run djob.JobRun) (djob.Driver, error) {
		if run.Binding == nil {
			return nil, fmt.Errorf("measurement requires a binding")
		}
		var config struct {
			Protocol   string          `json:"protocol"`
			ChannelID  string          `json:"channel_id"`
			Operations json.RawMessage `json:"operations"`
		}
		if json.Unmarshal(run.Binding.Config, &config) != nil {
			return nil, fmt.Errorf("invalid measurement binding")
		}
		factory, err := modbusdriver.NewFactory(config.Protocol, recovery)
		if err != nil {
			return nil, err
		}
		// Protocol selection belongs to this company integration. The supplied
		// static Modbus encoder receives only its own documented configuration.
		binding := *run.Binding
		binding.Config, _ = json.Marshal(struct {
			ChannelID  string          `json:"channel_id"`
			Operations json.RawMessage `json:"operations"`
		}{config.ChannelID, config.Operations})
		run.Binding = &binding
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
		effect.Status, effect.Error, effect.Data = djob.EffectFailed, "measurement integration requires one register", nil
		return effect
	}
	effect.Data, _ = json.Marshal(measurement.Reading{Value: int(result.Registers[0])})
	return effect
}
