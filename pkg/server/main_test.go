package server_test

import (
	"os"
	"testing"

	"github.com/duravela/djob/pkg/server"
	"github.com/duravela/djob/pkg/testutils/serverutils"
)

func TestMain(m *testing.M) {
	serverutils.InitTestServerFactory(server.TestServerFactory)

	os.Exit(m.Run())
}
