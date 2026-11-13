package jobs

import "github.com/duravela/djob/sdk/go/handlers"

type RequestReplyInput = handlers.RequestReplyInput

// Builtins returns the supplied protocol-agnostic SDK handlers.
func Builtins() Registry { return handlers.Builtins() }
