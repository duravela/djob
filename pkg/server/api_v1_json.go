package server

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	gateway "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const maxAPIRequestBytes = 128 << 10

func invalidHTTP(message string) error { return status.Error(codes.InvalidArgument, message) }

func writeAPIError(w http.ResponseWriter, err error) {
	s := status.Convert(apiError(err))
	data, _ := protojson.Marshal(s.Proto())
	w.Header().Set("Content-Type", "application/json")
	if s.Code() == codes.Unauthenticated {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	w.WriteHeader(gateway.HTTPStatusFromCode(s.Code()))
	_, _ = w.Write(data)
}

func gatewayAPIError(_ context.Context, _ *gateway.ServeMux, _ gateway.Marshaler, w http.ResponseWriter, _ *http.Request, err error) {
	// Both HTTP surfaces use the same JSON error contract and Bearer challenge.
	writeAPIError(w, err)
}

func jsonRPC[I proto.Message, O proto.Message](makeInput func() I, call func(context.Context, I) (O, error), bind func(*http.Request, I) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		input := makeInput()
		if r.Method != http.MethodGet {
			kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || kind != "application/json" {
				writeAPIError(w, invalidHTTP("Content-Type must be application/json"))
				return
			}
			data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAPIRequestBytes))
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					writeAPIError(w, status.Error(codes.ResourceExhausted, "request body exceeds 128 KiB"))
				} else {
					writeAPIError(w, invalidHTTP("cannot read request body"))
				}
				return
			}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, input); err != nil {
				writeAPIError(w, invalidHTTP(err.Error()))
				return
			}
		}
		if err := bind(r, input); err != nil {
			writeAPIError(w, err)
			return
		}
		output, err := call(r.Context(), input)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(output)
		if err != nil {
			writeAPIError(w, status.Error(codes.Internal, "cannot encode response"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}
}

func pathID(r *http.Request, id *string) error {
	if len(r.URL.Query()) != 0 {
		return invalidHTTP("query parameters are unsupported on this resource")
	}
	path := mux.Vars(r)["id"]
	if *id != "" && *id != path {
		return invalidHTTP("resource ID in body must match URL")
	}
	*id = path
	return nil
}

func pageQuery(r *http.Request, size *uint32, token *string) error {
	q := r.URL.Query()
	for key, values := range q {
		if (key != "page_size" && key != "page_token") || len(values) != 1 {
			return invalidHTTP("use one page_size and one page_token parameter")
		}
	}
	if value := q.Get("page_size"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return invalidHTTP("invalid page_size")
		}
		*size = uint32(parsed)
	}
	*token = q.Get("page_token")
	return nil
}
