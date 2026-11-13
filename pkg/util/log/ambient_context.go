// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package log

import "context"

// AmbientContext preserves the configuration API. djob installs no ambient tags.
type AmbientContext struct{}

func (*AmbientContext) AnnotateCtx(ctx context.Context) context.Context { return ctx }
