package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsoleCLIHelpVersionAndTransportValidation(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"version"}} {
		var output bytes.Buffer
		require.NoError(t, run(context.Background(), args, &output))
		require.NotEmpty(t, output.String())
	}
	for _, args := range [][]string{
		{"--listen", "0.0.0.0:8090"},
		{"--controllers", "http://controller.example.com:8080"},
		{"--controllers", "https://example.com/private"},
		{"--public-url", "http://localhost:8090"},
		{"--tls-cert", "only-one.pem"},
		{"--session-ttl", "25h"},
		{"--max-sessions", "1025"},
		{"unexpected"},
	} {
		t.Run(args[0]+args[len(args)-1], func(t *testing.T) {
			require.Error(t, run(context.Background(), args, new(bytes.Buffer)))
		})
	}
}
