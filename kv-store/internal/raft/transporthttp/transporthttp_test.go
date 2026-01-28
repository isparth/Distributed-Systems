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
	lastReq  AppendEntriesRequest
	respTerm uint64
}

func (m *mockHandler) HandleAppendEntries(_ context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	m.lastReq = req
	return AppendEntriesResponse{Term: m.respTerm, Success: true}, nil
}

func TestTransportHTTP_AppendEntries_RoundTrip(t *testing.T) {
	handler := &mockHandler{respTerm: 3}
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
	if handler.lastReq.LeaderID != "node1" {
		t.Fatalf("expected leader node1, got %s", handler.lastReq.LeaderID)
	}
	if len(handler.lastReq.Entries) != 1 || handler.lastReq.Entries[0].Cmd.Key != "k" {
		t.Fatalf("entries mismatch: %+v", handler.lastReq.Entries)
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
