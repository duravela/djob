// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/duravela/djob/pkg/server/apiconstants"
)

type httpServer struct {
	mux        http.ServeMux
	compressed http.Handler
}

func newHTTPServer(_ BaseConfig) *httpServer {
	s := new(httpServer)
	s.compressed = gziphandler.GzipHandler(&s.mux)
	return s
}

func (s *httpServer) setupRoutes(_ context.Context, api http.Handler) error {
	if api != nil {
		s.mux.Handle(apiconstants.APIV1Path, api)
	}
	return nil
}

func (s *httpServer) baseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	defer func() {
		if value := recover(); value != nil {
			if value == http.ErrAbortHandler {
				panic(value)
			}
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}()
	s.compressed.ServeHTTP(w, r)
}
