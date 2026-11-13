// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
)

// ErrSessionLost identifies a transient connection, session or execution
// permission loss. It never authorizes resuming that session or replaying I/O.
var ErrSessionLost = errors.New("worker connection or execution permission lost")

// ErrCleanupFailed prevents Serve from reconnecting after a reported Close
// failure or unconfirmed recovery/close following session loss. A later local
// cleanup retry cannot erase a reported Close failure. Intervention is required.
var ErrCleanupFailed = errors.New("worker driver cleanup failed")

type responseError struct {
	status    int
	transient bool
}

func (e *responseError) Error() string {
	return fmt.Sprintf("worker request rejected (HTTP %d)", e.status)
}
func (e *responseError) Unwrap() error {
	if e.transient {
		return ErrSessionLost
	}
	return nil
}

func rejected(path string, status int) error {
	transient := status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504
	// A missing registration route is a configuration error. An already ended
	// session requires fresh registration and can never be polled again.
	if strings.Contains(path, "/") && (status == 404 || status == 410) {
		transient = true
	}
	return &responseError{status: status, transient: transient}
}

func transportError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	var verification *tls.CertificateVerificationError
	var authority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var certificate x509.CertificateInvalidError
	var record tls.RecordHeaderError
	var alert *net.OpError
	if errors.As(err, &verification) || errors.As(err, &authority) || errors.As(err, &hostname) || errors.As(err, &certificate) || errors.As(err, &record) || (errors.As(err, &alert) && alert.Op == "remote error") {
		return fmt.Errorf("worker TLS handshake or server verification failed")
	}
	// Do not retain wrapped URLs, credentials or proxy text in returned errors.
	return ErrSessionLost
}
