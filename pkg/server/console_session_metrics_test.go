package server

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestSessionMetricsUseCachedStorageAndBoundedRejectionLabels(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	s := startAPI(t, cfg)
	activateControllerSessions(t, s)
	_ = createControllerSession(t, s, time.Hour)
	s.jobs.sessions.sampleMetrics()
	before := s.application.Store.LastAppliedIndex()
	families, err := s.metrics.registry.Gather()
	require.NoError(t, err)
	found := false
	for _, family := range families {
		if family.GetName() == "djob_console_session_storage" {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetName() == "kind" && label.GetValue() == "records" {
						require.Equal(t, float64(1), metric.GetGauge().GetValue())
						found = true
					}
				}
			}
		}
	}
	require.True(t, found)
	require.Equal(t, before, s.application.Store.LastAppliedIndex())
	s.jobs.sessions.writes = rate.NewLimiter(0, 0)
	response := sessionHTTP(t, s, genericToken("alpha/writer"), "create", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 429, response.StatusCode)
	families, err = s.metrics.registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "djob_console_session_rejections_total" {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetValue() == "ResourceExhausted" {
						require.Equal(t, float64(1), metric.GetCounter().GetValue())
					}
				}
			}
		}
	}
}
