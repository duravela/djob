package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	gateway "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func apiCredentials() *APIAuthConfig {
	return &APIAuthConfig{Credentials: []APICredential{
		{Subject: "reader", Role: APIReader, Token: strings.Repeat("r", 40)},
		{Subject: "writer", Role: APIWriter, Token: strings.Repeat("w", 40)},
		{Subject: "ops/alice", Role: APIOperator, Token: strings.Repeat("a", 40)},
		{Subject: "ops/alice", Role: APIOperator, Token: strings.Repeat("b", 40)},
		{Subject: "ops/bob", Role: APIOperator, Token: strings.Repeat("c", 40)},
	}}
}

func apiAuthContext(token string) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func TestEverySchedulerMethodAuthenticatesBeforeAccess(t *testing.T) {
	s, err := NewSchedulerServiceWithAuth(nil, *apiCredentials())
	require.NoError(t, err)
	for _, method := range pb.Scheduler_ServiceDesc.Methods {
		t.Run(method.MethodName, func(t *testing.T) {
			_, err := method.Handler(s, context.Background(), func(any) error { return nil }, nil)
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		})
	}
}

func TestServiceRejectsAmbiguousCredentialsAndEnforcesRoles(t *testing.T) {
	svc, err := NewSchedulerServiceWithAuth(nil, *apiCredentials())
	require.NoError(t, err)
	s := svc.(*schedulerServer)
	for _, values := range [][]string{nil, {"Basic " + strings.Repeat("a", 40)}, {"Bearer short"}, {"Bearer " + strings.Repeat("z", 40)},
		{"Bearer " + strings.Repeat("a", 40), "Bearer " + strings.Repeat("a", 40)}, {"Bearer  " + strings.Repeat("a", 40)},
		{"Bearer " + strings.Repeat("a", 40) + ",Bearer " + strings.Repeat("a", 40)}} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{"authorization": values, "actor": {"admin"}})
		_, err := s.authorize(ctx, APIOperator)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotContains(t, err.Error(), strings.Repeat("a", 40))
	}
	for _, credential := range apiCredentials().Credentials {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "bEaReR "+credential.Token, "actor", "forged"))
		for _, role := range []APIRole{APIReader, APIWriter, APIOperator} {
			actor, err := s.authorize(ctx, role)
			if roleLevel(credential.Role) < roleLevel(role) {
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, credential.Subject, actor)
			}
		}
	}
	for _, method := range []string{"ExpireChannel", "ResolveChannel", "ResolveRun"} {
		for _, descriptor := range pb.Scheduler_ServiceDesc.Methods {
			if descriptor.MethodName == method {
				_, err := descriptor.Handler(NewSchedulerService(nil), context.Background(), func(any) error { return nil }, nil)
				require.Equal(t, codes.Unauthenticated, status.Code(err), "anonymous embedding must not enable recovery")
			}
		}
	}
}

func TestCredentialFileValidationNeverEchoesTokens(t *testing.T) {
	secret := strings.Repeat("s", 40)
	for name, data := range map[string]string{
		"empty": "", "null": "null", "empty credentials": `{"credentials":[]}`,
		"unknown field": `{"credentials":[],"secret":"` + secret + `"}`,
		"trailing JSON": `{"credentials":[]} {}`, "oversize": strings.Repeat(" ", (64<<10)+1),
		"bad role":        `{"credentials":[{"subject":"ops","role":"` + secret + `","token":"` + secret + `"}]}`,
		"bad subject":     `{"credentials":[{"subject":"ops\nadmin","role":"operator","token":"` + secret + `"}]}`,
		"bad token":       `{"credentials":[{"subject":"ops","role":"operator","token":"` + secret + `!"}]}`,
		"duplicate token": `{"credentials":[{"subject":"ops","role":"reader","token":"` + secret + `"},{"subject":"admin","role":"operator","token":"` + secret + `"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth.json")
			require.NoError(t, os.WriteFile(path, []byte(data), 0600))
			_, err := LoadAPIAuthConfig(path)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret)
		})
	}
	path := filepath.Join(t.TempDir(), "valid.json")
	data, err := json.Marshal(apiCredentials())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	cfg, err := LoadAPIAuthConfig(path)
	require.NoError(t, err)
	require.Equal(t, apiCredentials(), cfg)
	_, err = newAPIAuthorizer(&APIAuthConfig{Credentials: make([]APICredential, 129)})
	require.Error(t, err)
}

func authenticatedHTTP(t *testing.T, client *http.Client, endpoint, method, path, token string, input proto.Message, output proto.Message, want int, extra http.Header) []byte {
	t.Helper()
	var data []byte
	var err error
	if input != nil {
		data, err = (protojson.MarshalOptions{UseProtoNames: true}).Marshal(input)
		require.NoError(t, err)
	}
	r, err := http.NewRequest(method, endpoint+path, bytes.NewReader(data))
	require.NoError(t, err)
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for k, values := range extra {
		for _, v := range values {
			r.Header.Add(k, v)
		}
	}
	response, err := client.Do(r)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, want, response.StatusCode, string(data))
	if want == http.StatusUnauthorized {
		require.Equal(t, "Bearer", response.Header.Get("WWW-Authenticate"))
	}
	if output != nil {
		require.NoError(t, protojson.Unmarshal(data, output), string(data))
	}
	return data
}

func TestAPICredentialsCoverGRPCRESTAndJSONRPC(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = apiCredentials()
	s := startAPI(t, cfg)
	require.Nil(t, s.cfg.APIAuth, "the live config retains no raw credentials")
	c := apiClient(t, s)
	_, err := c.ListChannels(context.Background(), &pb.ListChannelsRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	writerToken := apiCredentials().Credentials[1].Token
	readerToken := apiCredentials().Credentials[0].Token
	_, err = c.ConfigureChannel(apiAuthContext(readerToken), &pb.ConfigureChannelRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = c.ConfigureChannel(apiAuthContext(writerToken), &pb.ConfigureChannelRequest{Command: meta("configured"), Id: "gateway", GatewayId: "gateway", Protocol: "demo", Endpoint: "demo://echo"})
	require.NoError(t, err)
	_, err = c.ResolveChannel(apiAuthContext(writerToken), &pb.ResolveChannelRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	// Mutating the caller's config cannot change the running authentication map.
	cfg.APIAuth.Credentials[0] = APICredential{Subject: "forged", Role: APIOperator, Token: strings.Repeat("z", 40)}
	_, err = c.ListChannels(apiAuthContext(readerToken), &pb.ListChannelsRequest{})
	require.NoError(t, err)
	_, err = c.ListChannels(apiAuthContext(strings.Repeat("z", 40)), &pb.ListChannelsRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	endpoint := "http://" + s.HTTPAddr()
	for _, path := range []string{"/api/v1/channels/", "/scheduler.Scheduler/ListChannels"} {
		method := http.MethodGet
		var input proto.Message
		if strings.HasPrefix(path, "/scheduler.") {
			method, input = http.MethodPost, &pb.ListChannelsRequest{}
		}
		authenticatedHTTP(t, http.DefaultClient, endpoint, method, path, "", input, nil, 401, http.Header{"Grpc-Metadata-Authorization": {"Bearer " + readerToken}, "X-Forwarded-User": {"admin"}})
		var result pb.ListChannelsResponse
		authenticatedHTTP(t, http.DefaultClient, endpoint, method, path, readerToken, input, &result, 200, nil)
		require.Len(t, result.Channels, 1)
		authenticatedHTTP(t, http.DefaultClient, endpoint, method, path, readerToken, input, nil, 401, http.Header{"Authorization": {"Bearer " + readerToken}})
	}
	authenticatedHTTP(t, http.DefaultClient, endpoint, http.MethodPost, "/api/v1/runs/another/cancel", readerToken, &pb.CancelRunRequest{Command: meta("cancel")}, nil, 403, nil)
	authenticatedHTTP(t, http.DefaultClient, endpoint, http.MethodGet, "/healthz", "", nil, nil, 200, nil)
}

func TestEmbeddedJSONGatewayUsesServiceAuthentication(t *testing.T) {
	s, err := NewSchedulerServiceWithAuth(nil, *apiCredentials())
	require.NoError(t, err)
	mux := gateway.NewServeMux(gateway.WithErrorHandler(gatewayAPIError))
	require.NoError(t, pb.RegisterSchedulerHandlerServer(context.Background(), mux, s))
	server := httptest.NewServer(SchedulerHTTPHandler(mux))
	defer server.Close()
	token := apiCredentials().Credentials[2].Token
	authenticatedHTTP(t, server.Client(), server.URL, http.MethodPost, "/scheduler.Scheduler/ResolveChannel", "", &pb.ResolveChannelRequest{}, nil, 401,
		http.Header{"Grpc-Metadata-Authorization": {"Bearer " + token}, "Grpc-Metadata-Actor": {"admin"}})
	authenticatedHTTP(t, server.Client(), server.URL, http.MethodPost, "/scheduler.Scheduler/ResolveChannel", token, &pb.ResolveChannelRequest{}, nil, 400, nil)
}
