package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/storage"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/transporthttp"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

func makeLeaderNode(t *testing.T, peers []types.NodeID, tp transporthttp.Transport) (*Node, *kvsm.KVStateMachine) {
	t.Helper()
	sm := kvsm.New()
	stable := storage.NewMemStableStore()
	stable.SetCurrentTerm(1)
	log := storage.NewMemLogStore()
	snap := storage.NewMemSnapshotStore()
	cfg := Config{ID: "leader", Peers: peers, Addr: "http://leader:8080", Role: RoleLeader}
	n, err := NewNode(cfg, stable, log, snap, tp, sm)
	if err != nil {
		t.Fatal(err)
	}
	return n, sm
}

func makeFollowerNode(t *testing.T, id types.NodeID) (*Node, *kvsm.KVStateMachine) {
	t.Helper()
	sm := kvsm.New()
	stable := storage.NewMemStableStore()
	log := storage.NewMemLogStore()
	snap := storage.NewMemSnapshotStore()
	cfg := Config{ID: id, Role: RoleFollower}
	n, err := NewNode(cfg, stable, log, snap, nil, sm)
	if err != nil {
		t.Fatal(err)
	}
	return n, sm
}

func TestRaft_M1_LeaderPropose_AppendsAndCommits(t *testing.T) {
	// Create two followers with HTTP servers
	f1, f1sm := makeFollowerNode(t, "f1")
	f2, f2sm := makeFollowerNode(t, "f2")

	ctx := context.Background()
	f1.Start(ctx)
	f2.Start(ctx)
	defer f1.Stop(ctx)
	defer f2.Stop(ctx)

	ts1 := httptest.NewServer(f1.RaftHTTPHandler().Handler())
	defer ts1.Close()
	ts2 := httptest.NewServer(f2.RaftHTTPHandler().Handler())
	defer ts2.Close()

	resolver := transporthttp.NewPeerResolver(map[types.NodeID]string{
		"f1": ts1.URL,
		"f2": ts2.URL,
	})
	tp := transporthttp.NewHTTPTransport(resolver)

	leader, leaderSM := makeLeaderNode(t, []types.NodeID{"f1", "f2"}, tp)
	leader.Start(ctx)
	defer leader.Stop(ctx)

	cmd := types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "hello", Value: "world"}
	res, err := leader.Propose(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Ok {
		t.Fatalf("propose failed: %+v", res)
	}

	// Leader SM should have the value
	v, ok := leaderSM.Get("hello")
	if !ok || v != "world" {
		t.Fatalf("leader sm: expected world, got %q ok=%v", v, ok)
	}

	// Wait for followers to apply
	time.Sleep(50 * time.Millisecond)
	v, ok = f1sm.Get("hello")
	if !ok || v != "world" {
		t.Fatalf("f1 sm: expected world, got %q ok=%v", v, ok)
	}
	v, ok = f2sm.Get("hello")
	if !ok || v != "world" {
		t.Fatalf("f2 sm: expected world, got %q ok=%v", v, ok)
	}
}

func TestRaft_M1_FollowerHandleAppendEntries_AppendsAndApplies(t *testing.T) {
	follower, sm := makeFollowerNode(t, "f1")
	ctx := context.Background()
	follower.Start(ctx)
	defer follower.Stop(ctx)

	req := transporthttp.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader",
		LeaderAddr:   "http://leader:8080",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []storage.LogEntry{
			{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k1", Value: "v1"}},
		},
		LeaderCommit: 1,
	}

	resp, err := follower.HandleAppendEntries(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("expected success")
	}

	// Wait for applier
	time.Sleep(50 * time.Millisecond)
	v, ok := sm.Get("k1")
	if !ok || v != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}

	// Verify leader hint was set
	hint := follower.LeaderHint()
	if hint.LeaderID != "leader" {
		t.Fatalf("expected leader hint, got %+v", hint)
	}
}

// --- Integration tests ---

type clusterNode struct {
	node   *Node
	sm     *kvsm.KVStateMachine
	server *httptest.Server
}

func setupCluster(t *testing.T, n int) (leader *clusterNode, followers []*clusterNode, cleanup func()) {
	t.Helper()
	ctx := context.Background()

	// Create followers first to get their URLs
	followers = make([]*clusterNode, n-1)
	peerMap := make(map[types.NodeID]string)
	peerIDs := make([]types.NodeID, n-1)

	for i := range followers {
		id := types.NodeID(fmt.Sprintf("f%d", i+1))
		peerIDs[i] = id
		f, sm := makeFollowerNode(t, id)
		f.Start(ctx)
		ts := httptest.NewServer(f.RaftHTTPHandler().Handler())
		peerMap[id] = ts.URL
		followers[i] = &clusterNode{node: f, sm: sm, server: ts}
	}

	resolver := transporthttp.NewPeerResolver(peerMap)
	tp := transporthttp.NewHTTPTransport(resolver)
	leaderNode, leaderSM := makeLeaderNode(t, peerIDs, tp)
	leaderNode.Start(ctx)
	leaderTS := httptest.NewServer(leaderNode.RaftHTTPHandler().Handler())

	leader = &clusterNode{node: leaderNode, sm: leaderSM, server: leaderTS}

	cleanup = func() {
		leaderTS.Close()
		leaderNode.Stop(ctx)
		for _, f := range followers {
			f.server.Close()
			f.node.Stop(ctx)
		}
	}

	return leader, followers, cleanup
}

func TestCluster_M1_3Nodes_WriteToLeader_ReplicatesToFollowers(t *testing.T) {
	leader, followers, cleanup := setupCluster(t, 3)
	defer cleanup()

	ctx := context.Background()
	cmd := types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "x", Value: "42"}
	res, err := leader.node.Propose(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Ok {
		t.Fatalf("propose failed: %+v", res)
	}

	// Leader has it
	v, ok := leader.sm.Get("x")
	if !ok || v != "42" {
		t.Fatalf("leader: expected 42, got %q", v)
	}

	// Wait for followers
	time.Sleep(50 * time.Millisecond)
	for i, f := range followers {
		v, ok := f.sm.Get("x")
		if !ok || v != "42" {
			t.Fatalf("follower %d: expected 42, got %q ok=%v", i, v, ok)
		}
	}
}

func TestCluster_M1_WriteToFollower_ReturnsRedirect(t *testing.T) {
	leader, followers, cleanup := setupCluster(t, 3)
	defer cleanup()

	// Set up the follower's leader hint by sending a heartbeat-like AppendEntries
	ctx := context.Background()
	followers[0].node.HandleAppendEntries(ctx, transporthttp.AppendEntriesRequest{
		Term:       1,
		LeaderID:   "leader",
		LeaderAddr: leader.server.URL,
	})

	// Try to propose on a follower — should get ErrNotLeader
	cmd := types.Command{Op: types.OpPut, Key: "y", Value: "99"}
	_, err := followers[0].node.Propose(ctx, cmd)
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}

	// Also test via HTTP: set up DKV + HTTP server on the follower
	// and verify 307 redirect
	dkvFollower := setupDKVHTTP(t, followers[0])
	defer dkvFollower.Close()

	putBody, _ := json.Marshal(map[string]interface{}{
		"client_id": "c1", "seq": 1, "value": "test",
	})
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	req, _ := http.NewRequest(http.MethodPut, dkvFollower.URL+"/kv/testkey", bytes.NewReader(putBody))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 307 {
		t.Fatalf("expected 307, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if body["error"] != "not_leader" {
		t.Fatalf("expected not_leader error, got %v", body)
	}
}

func setupDKVHTTP(t *testing.T, cn *clusterNode) *httptest.Server {
	t.Helper()
	// Import at test level to avoid circular deps — use httpapi + distributedkv
	// We'll build this inline since we can't import httpapi from raft package.
	// Instead, we test the redirect at the Raft level (Propose returns ErrNotLeader).
	// For actual HTTP redirect testing, we use the httpapi test package.
	// Here we create a minimal HTTP handler.
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if !cn.node.IsLeader() {
			hint := cn.node.LeaderHint()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(307)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":       "not_leader",
				"leader_hint": hint,
			})
			return
		}
	})
	return httptest.NewServer(mux)
}
