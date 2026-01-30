package transporthttp

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/storage"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// mockHandler implements RaftRPCHandler for testing.
type mockHandler struct {
	lastAEReq  AppendEntriesRequest
	lastRVReq  RequestVoteRequest
	lastISReq  InstallSnapshotRequest
	aeRespTerm uint64
	rvRespTerm uint64
	isRespTerm uint64
	voteGrant  bool
}

func (m *mockHandler) HandleAppendEntries(_ context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	m.lastAEReq = req
	return AppendEntriesResponse{Term: m.aeRespTerm, Success: true}, nil
}

func (m *mockHandler) HandleRequestVote(_ context.Context, req RequestVoteRequest) (RequestVoteResponse, error) {
	m.lastRVReq = req
	return RequestVoteResponse{Term: m.rvRespTerm, VoteGranted: m.voteGrant}, nil
}

// M5: HandleInstallSnapshot for mock handler
func (m *mockHandler) HandleInstallSnapshot(_ context.Context, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	m.lastISReq = req
	return InstallSnapshotResponse{Term: m.isRespTerm}, nil
}

func TestTransportHTTP_AppendEntries_RoundTrip(t *testing.T) {
	handler := &mockHandler{aeRespTerm: 3}
	raftSrv := NewRaftHTTPServer(handler)
	ts := httptest.NewServer(raftSrv.Handler())
	defer ts.Close()

	resolver := NewPeerResolver(map[types.NodeID]string{
		"node2": ts.URL,
	})
	transport := NewHTTPTransport(resolver)

	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     "node1",
		LeaderAddr:   "http://localhost:8080",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []storage.LogEntry{
			{Index: 1, Term: 3, Cmd: types.Command{Op: types.OpPut, Key: "k", Value: "v"}},
		},
		LeaderCommit: 0,
	}

	resp, err := transport.AppendEntries(context.Background(), "node2", req)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("expected success")
	}
	if resp.Term != 3 {
		t.Fatalf("expected term 3, got %d", resp.Term)
	}
	if handler.lastAEReq.LeaderID != "node1" {
		t.Fatalf("expected leader node1, got %s", handler.lastAEReq.LeaderID)
	}
	if len(handler.lastAEReq.Entries) != 1 || handler.lastAEReq.Entries[0].Cmd.Key != "k" {
		t.Fatalf("entries mismatch: %+v", handler.lastAEReq.Entries)
	}
}

func TestTransportHTTP_BadJSON_Returns400(t *testing.T) {
	handler := &mockHandler{}
	raftSrv := NewRaftHTTPServer(handler)
	ts := httptest.NewServer(raftSrv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/raft/append_entries", "application/json", strings.NewReader("{invalid"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestTransportHTTP_RequestVote_RoundTrip(t *testing.T) {
	handler := &mockHandler{rvRespTerm: 5, voteGrant: true}
	raftSrv := NewRaftHTTPServer(handler)
	ts := httptest.NewServer(raftSrv.Handler())
	defer ts.Close()

	resolver := NewPeerResolver(map[types.NodeID]string{
		"node2": ts.URL,
	})
	transport := NewHTTPTransport(resolver)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  "node1",
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	resp, err := transport.RequestVote(context.Background(), "node2", req)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.VoteGranted {
		t.Fatal("expected vote granted")
	}
	if resp.Term != 5 {
		t.Fatalf("expected term 5, got %d", resp.Term)
	}
	if handler.lastRVReq.CandidateID != "node1" {
		t.Fatalf("expected candidate node1, got %s", handler.lastRVReq.CandidateID)
	}
	if handler.lastRVReq.LastLogIndex != 10 || handler.lastRVReq.LastLogTerm != 4 {
		t.Fatalf("request mismatch: %+v", handler.lastRVReq)
	}
}

// --- M5 Tests ---

func TestTransportHTTP_InstallSnapshot_Base64RoundTrip(t *testing.T) {
	handler := &mockHandler{isRespTerm: 7}
	raftSrv := NewRaftHTTPServer(handler)
	ts := httptest.NewServer(raftSrv.Handler())
	defer ts.Close()

	resolver := NewPeerResolver(map[types.NodeID]string{
		"node2": ts.URL,
	})
	transport := NewHTTPTransport(resolver)

	// Create a base64-encoded snapshot
	// snapshotData would be: `{"kv":{"foo":"bar","baz":"qux"},"dedupe":{}}`
	encodedData := "eyJrdiI6eyJmb28iOiJiYXIiLCJiYXoiOiJxdXgifSwiZGVkdXBlIjp7fX0=" // base64 of snapshotData

	req := InstallSnapshotRequest{
		Term:              7,
		LeaderID:          "node1",
		LeaderAddr:        "http://localhost:8080",
		LastIncludedIndex: 100,
		LastIncludedTerm:  6,
		Data:              encodedData,
	}

	resp, err := transport.InstallSnapshot(context.Background(), "node2", req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Term != 7 {
		t.Fatalf("expected term 7, got %d", resp.Term)
	}

	// Verify the request was received correctly
	if handler.lastISReq.LeaderID != "node1" {
		t.Fatalf("expected leader node1, got %s", handler.lastISReq.LeaderID)
	}
	if handler.lastISReq.LastIncludedIndex != 100 {
		t.Fatalf("expected last included index 100, got %d", handler.lastISReq.LastIncludedIndex)
	}
	if handler.lastISReq.LastIncludedTerm != 6 {
		t.Fatalf("expected last included term 6, got %d", handler.lastISReq.LastIncludedTerm)
	}
	if handler.lastISReq.Data != encodedData {
		t.Fatalf("data mismatch: got %s", handler.lastISReq.Data)
	}
}
