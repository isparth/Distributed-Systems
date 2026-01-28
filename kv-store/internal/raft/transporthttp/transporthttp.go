package transporthttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/storage"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// --- RPC DTOs ---

type AppendEntriesRequest struct {
	Term         uint64             `json:"term"`
	LeaderID     types.NodeID       `json:"leader_id"`
	LeaderAddr   string             `json:"leader_addr"`
	PrevLogIndex uint64             `json:"prev_log_index"`
	PrevLogTerm  uint64             `json:"prev_log_term"`
	Entries      []storage.LogEntry `json:"entries"`
	LeaderCommit uint64             `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
}

// --- Interfaces ---

// RaftRPCHandler is implemented by the Raft node to handle incoming RPCs.
type RaftRPCHandler interface {
	HandleAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

// Transport is the interface the Raft node uses to send RPCs.
type Transport interface {
	AppendEntries(ctx context.Context, to types.NodeID, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

// --- PeerResolver ---

// PeerResolver maps NodeID to network address.
type PeerResolver struct {
	peers map[types.NodeID]string
}

func NewPeerResolver(peers map[types.NodeID]string) *PeerResolver {
	return &PeerResolver{peers: peers}
}

func (r *PeerResolver) Resolve(id types.NodeID) (string, error) {
	addr, ok := r.peers[id]
	if !ok {
		return "", fmt.Errorf("unknown peer: %s", id)
	}
	return addr, nil
}

// --- HTTPTransport (client) ---

type HTTPTransport struct {
	resolver *PeerResolver
	client   *http.Client
}

func NewHTTPTransport(resolver *PeerResolver) *HTTPTransport {
	return &HTTPTransport{
		resolver: resolver,
		client:   &http.Client{},
	}
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, to types.NodeID, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	addr, err := t.resolver.Resolve(to)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	body, err := json.Marshal(req)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/raft/append_entries", bytes.NewReader(body))
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return AppendEntriesResponse{}, fmt.Errorf("append_entries to %s returned %d", to, resp.StatusCode)
	}

	var result AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return AppendEntriesResponse{}, err
	}
	return result, nil
}

// --- RaftHTTPServer (server mux) ---

type RaftHTTPServer struct {
	handler RaftRPCHandler
}

func NewRaftHTTPServer(handler RaftRPCHandler) *RaftHTTPServer {
	return &RaftHTTPServer{handler: handler}
}

func (s *RaftHTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/append_entries", s.handleAppendEntries)
	return mux
}

func (s *RaftHTTPServer) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "bad JSON"})
		return
	}

	resp, err := s.handler.HandleAppendEntries(r.Context(), req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
