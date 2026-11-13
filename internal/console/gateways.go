package console

import (
	"net/http"
	"strings"
	"unicode/utf8"

	pb "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) gatewayQuery(w http.ResponseWriter, r *http.Request, sess *session) {
	if !platformSession(sess) {
		problem(w, 403, "gateway authority requires a platform operator")
		return
	}
	var q struct {
		ID string `json:"id"`
	}
	if decode(w, r, 1024, &q) != nil || strings.TrimSpace(q.ID) == "" || len(q.ID) > 256 {
		problem(w, 400, "provide the exact gateway or audit ID")
		return
	}
	endpoint := "GetChannel"
	var input proto.Message = &pb.GetChannelRequest{Id: q.ID}
	if r.PathValue("method") == "audit" {
		endpoint = "GetResolution"
		input = &pb.GetResolutionRequest{RequestId: q.ID}
	} else if r.PathValue("method") != "get" {
		problem(w, 404, "unknown gateway query")
		return
	}
	data, _ := protojson.Marshal(input)
	sess.mu.Lock()
	index := sess.controller
	sess.mu.Unlock()
	response, index, err := s.request(r.Context(), sess.credential(), index, "legacy/"+endpoint, http.MethodPost, data, true)
	if err != nil {
		problem(w, 502, err.Error())
		return
	}
	sess.mu.Lock()
	sess.controller = index
	sess.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.status)
	w.Write(response.body)
}
func (s *Server) prepareGateway(w http.ResponseWriter, r *http.Request, sess *session) {
	if !platformSession(sess) {
		problem(w, 403, "gateway authority requires a platform operator")
		return
	}
	var q struct {
		Operation string `json:"operation"`
		ID        string `json:"id"`
		Epoch     uint64 `json:"expected_epoch,string"`
		Reason    string `json:"reason"`
		Quiescent bool   `json:"quiescent"`
	}
	if decode(w, r, 8192, &q) != nil || strings.TrimSpace(q.ID) == "" || len(q.ID) > 256 || !utf8.ValidString(q.ID) || q.Epoch == 0 || (q.Operation != "resolve" && q.Operation != "expire") {
		problem(w, 400, "provide the gateway ID, operation and exact positive epoch")
		return
	}
	if q.Operation == "resolve" && (!q.Quiescent || strings.TrimSpace(q.Reason) == "" || len(q.Reason) > 4096 || !utf8.ValidString(q.Reason)) {
		problem(w, 400, "physical resolution requires confirmed quiescence and an audited reason")
		return
	}
	id, err := randomID()
	if err != nil {
		problem(w, 500, "cannot create review")
		return
	}
	command := &pb.CommandMetadata{RequestId: "console/" + id, At: timestamppb.Now()}
	endpoint := "ExpireChannel"
	var input proto.Message = &pb.ExpireChannelRequest{Command: command, Id: q.ID, ExpectedEpoch: q.Epoch}
	if q.Operation == "resolve" {
		endpoint = "ResolveChannel"
		input = &pb.ResolveChannelRequest{Command: command, Id: q.ID, ExpectedEpoch: q.Epoch, Reason: q.Reason, Quiescent: true}
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(input)
	if err != nil {
		problem(w, 400, "invalid gateway review")
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.pending != nil {
		problem(w, 409, "finish the current review first")
		return
	}
	clearDraftLocked(sess)
	sess.pending = &pendingMutation{RequestID: command.RequestId, Request: data, Endpoint: "legacy/" + endpoint, State: "prepared", DraftRevision: sess.draftRevision}
	writeJSON(w, 200, sess.pending)
}
func gatewayAcknowledgementMatches(p *pendingMutation, data []byte) bool {
	var ack pb.ChannelResponse
	if protojson.Unmarshal(data, &ack) != nil || ack.Channel == nil {
		return false
	}
	var id string
	var epoch uint64
	var state string
	if p.Endpoint == "legacy/ResolveChannel" {
		var q pb.ResolveChannelRequest
		if protojson.Unmarshal(p.Request, &q) != nil {
			return false
		}
		id, epoch, state = q.Id, q.ExpectedEpoch, "available"
	} else {
		var q pb.ExpireChannelRequest
		if protojson.Unmarshal(p.Request, &q) != nil {
			return false
		}
		id, epoch, state = q.Id, q.ExpectedEpoch, "blocked"
	}
	return ack.Channel.Id == id && ack.Channel.Epoch == epoch && ack.Channel.State == state
}
