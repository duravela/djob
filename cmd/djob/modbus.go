package main

import (
	"fmt"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/modbusrtutcp"
	"github.com/duravela/djob/pkg/executor/modbustcp"
)

// Selection is channel configuration, not application job input. Each gateway
// still has one canonical channel regardless of framing or downstream unit ID.
func modbusFactory(tcpEnabled, rtuEnabled bool) executor.Factory {
	return func(channel db.CommunicationChannel) (executor.Transport, error) {
		switch {
		case tcpEnabled && channel.Protocol == modbustcp.Protocol:
			return modbustcp.Factory(channel)
		case rtuEnabled && channel.Protocol == modbusrtutcp.Protocol:
			return modbusrtutcp.Factory(channel)
		default:
			return nil, fmt.Errorf("the channel's Modbus transport is not enabled")
		}
	}
}
