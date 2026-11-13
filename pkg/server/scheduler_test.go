package server

import (
	"github.com/duravela/djob/pkg/testutils/serverutils"
	"google.golang.org/protobuf/proto"
)

var schedulerPrefix = "/_scheduler/v1/"

func getSchedulerJSONProto(
	ts serverutils.ApplicationLayerInterface, path string, response proto.Message,
) error {
	return getSchedulerJSONProtoWithSchedulerOption(ts, path, response, true)
}

func getSchedulerJSONProtoWithSchedulerOption(
	ts serverutils.ApplicationLayerInterface, path string, response proto.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithSchedulerOption(ts, adminPrefix+path, response)
}
