// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type failedPrefaceConn struct {
	net.Conn
	written chan struct{}
	read    chan struct{}
}

func (c *failedPrefaceConn) Write([]byte) (int, error) {
	close(c.written)
	return 0, io.ErrClosedPipe
}

func (c *failedPrefaceConn) Read([]byte) (int, error) {
	<-c.read
	return 0, &net.OpError{Op: "remote error", Net: "tls", Err: tls.AlertError(116)}
}

func TestFailedPrefacePreservesPendingTLSRefusal(t *testing.T) {
	conn := &failedPrefaceConn{written: make(chan struct{}), read: make(chan struct{})}
	observed := new(workerTLSObservation)
	secured := &workerTLSConn{Conn: conn, observation: observed, readDone: make(chan struct{})}
	returned := make(chan error, 1)
	go func() { _, err := secured.Write([]byte("preface")); returned <- err }()
	<-conn.written
	// Hold the alert until after the failed write. Returning now would allow
	// gRPC to close the connection before the reader can classify the refusal.
	select {
	case <-returned:
		t.Fatal("failed preface returned before the pending TLS alert was read")
	case <-time.After(20 * time.Millisecond):
	}
	readDone := make(chan struct{})
	go func() { _, _ = secured.Read(make([]byte, 1)); close(readDone) }()
	close(conn.read)
	select {
	case err := <-returned:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("write error changed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed write did not finish after the TLS alert")
	}
	<-readDone
	observed.mu.Lock()
	defer observed.mu.Unlock()
	if observed.failure == nil || errors.Is(observed.failure, ErrSessionLost) {
		t.Fatalf("TLS refusal lost: %v", observed.failure)
	}
}

func TestFailedPrefaceWaitIsBoundedWithoutAReader(t *testing.T) {
	conn := &failedPrefaceConn{written: make(chan struct{}), read: make(chan struct{})}
	secured := &workerTLSConn{Conn: conn, observation: new(workerTLSObservation), readDone: make(chan struct{})}
	done := make(chan struct{})
	go func() { _, _ = secured.Write([]byte("preface")); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failed connection startup waited indefinitely")
	}
}
