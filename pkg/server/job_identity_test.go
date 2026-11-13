package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobIdentityUsesActualCredentialAndRestrictsNonInspectionRoles(t *testing.T) {
	roles := []APIRole{APIReader, APIWriter, APIOperator, APICallback, APIProducer, APIWorker, APIMonitor}
	cfg := APIAuthConfig{}
	for _, role := range roles {
		tenant := "alpha"
		if role == APIMonitor {
			tenant = ""
		}
		credential := APICredential{Subject: string(role), Role: role, TenantID: tenant, Token: strings.Repeat(string(role)+"-", 8)}
		if role == APIProducer {
			credential.SourceID = "meters"
		}
		cfg.Credentials = append(cfg.Credentials, credential)
	}
	cfg.Credentials = append(cfg.Credentials, APICredential{Subject: "platform", Role: APIOperator, Token: strings.Repeat("platform-", 8)}, APICredential{Subject: "legacy", Role: APIReader, Token: strings.Repeat("legacy-", 8)})
	svc, err := NewJobsService(nil, cfg)
	require.NoError(t, err)
	handler := svc.(*jobsServer).HTTPHandler()
	for _, c := range append(cfg.Credentials, APICredential{}) {
		t.Run(c.Subject, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/jobs/v1/identity", nil)
			if c.Token != "" {
				r.Header.Set("Authorization", "Bearer "+c.Token)
			}
			r.Header.Set("Grpc-Metadata-Authorization", "Bearer "+cfg.Credentials[len(cfg.Credentials)-2].Token)
			r.Header.Set("X-Tenant-Id", "beta")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			switch {
			case c.Token == "":
				require.Equal(t, 401, w.Code)
			case c.Role == APIReader && c.TenantID != "" || c.Role == APIWriter || c.Role == APIOperator:
				require.Equal(t, 200, w.Code, w.Body.String())
				var identity map[string]string
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &identity))
				require.Equal(t, map[string]string{"subject": c.Subject, "role": string(c.Role), "tenant_id": c.TenantID}, identity)
				require.NotContains(t, w.Body.String(), c.Token)
			default:
				require.Equal(t, 403, w.Code, w.Body.String())
			}
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			require.Equal(t, "Authorization", w.Header().Get("Vary"))
		})
	}
}
