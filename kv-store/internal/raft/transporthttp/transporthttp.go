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

type RequestVoteRequest struct {
	Term         uint64       `json:"term"`
	CandidateID  types.NodeID `json:"candidate_id"`
	LastLogIndex uint64       `json:"last_log_index"`
	LastLogTerm  uint64       `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// M5: InstallSnapshot RPC DTOs
type InstallSnapshotRequest struct {
	Term              uint64       `json:"term"`
	LeaderID          types.NodeID `json:"leader_id"`
	LeaderAddr        string       `json:"leader_addr"`
	LastIncludedIndex uint64       `json:"last_included_index"`
	LastIncludedTerm  uint64       `json:"last_included_term"`
	Data              string       `json:"data"` // Base64-encoded snapshot
}

type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// --- Interfaces ---

// RaftRPCHandler is implemented by the Raft node to handle incoming RPCs.
type RaftRPCHandler interface {
	HandleAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error)
	HandleRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error)
	HandleInstallSnapshot(ctx context.Context, req InstallSnapshotRequest) (InstallSnapshotResponse, error) // M5
}

// Transport is the interface the Raft node uses to send RPCs.
type Transport interface {
	AppendEntries(ctx context.Context, to types.NodeID, req AppendEntriesRequest) (AppendEntriesResponse, error)
	RequestVote(ctx context.Context, to types.NodeID, req RequestVoteRequest) (RequestVoteResponse, error)
	InstallSnapshot(ctx context.Context, to types.NodeID, req InstallSnapshotRequest) (InstallSnapshotResponse, error) // M5
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

func (t *HTTPTransport) RequestVote(ctx context.Context, to types.NodeID, req RequestVoteRequest) (RequestVoteResponse, error) {
	addr, err := t.resolver.Resolve(to)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	body, err := json.Marshal(req)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/raft/request_vote", bytes.NewReader(body))
	if err != nil {
		return RequestVoteResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return RequestVoteResponse{}, fmt.Errorf("request_vote to %s returned %d", to, resp.StatusCode)
	}

	var result RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return RequestVoteResponse{}, err
	}
	return result, nil
}

// M5: InstallSnapshot sends a snapshot to a peer.
func (t *HTTPTransport) InstallSnapshot(ctx context.Context, to types.NodeID, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	addr, err := t.resolver.Resolve(to)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}

	body, err := json.Marshal(req)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/raft/install_snapshot", bytes.NewReader(body))
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return InstallSnapshotResponse{}, fmt.Errorf("install_snapshot to %s returned %d", to, resp.StatusCode)
	}

	var result InstallSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return InstallSnapshotResponse{}, err
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
	mux.HandleFunc("POST /raft/request_vote", s.handleRequestVote)
	mux.HandleFunc("POST /raft/install_snapshot", s.handleInstallSnapshot) // M5
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

func (s *RaftHTTPServer) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "bad JSON"})
		return
	}

	resp, err := s.handler.HandleRequestVote(r.Context(), req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// M5: handleInstallSnapshot handles incoming InstallSnapshot RPCs.
func (s *RaftHTTPServer) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	var req InstallSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "bad JSON"})
		return
	}

	resp, err := s.handler.HandleInstallSnapshot(r.Context(), req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
