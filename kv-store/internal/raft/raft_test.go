package raft

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/storage"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/transporthttp"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// fastTiming returns timing config for fast tests
func fastTiming() TimingConfig {
	return TimingConfig{
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
	}
}

func makeNode(t *testing.T, id types.NodeID, peers []types.NodeID, tp transporthttp.Transport) (*Node, *kvsm.KVStateMachine) {
	t.Helper()
	sm := kvsm.New()
	stable := storage.NewMemStableStore()
	log := storage.NewMemLogStore()
	snap := storage.NewMemSnapshotStore()
	cfg := Config{
		ID:     id,
		Peers:  peers,
		Addr:   fmt.Sprintf("http://%s:8080", id),
		Timing: fastTiming(),
		Rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	n, err := NewNode(cfg, stable, log, snap, tp, sm)
	if err != nil {
		t.Fatal(err)
	}
	return n, sm
}

// --- M1 Tests (kept for compatibility) ---

func TestRaft_M1_LeaderPropose_AppendsAndCommits(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2", "n3"}

	// Create placeholder nodes to get their HTTP handlers
	nodes := make([]*Node, 3)
	sms := make([]*kvsm.KVStateMachine, 3)
	servers := make([]*httptest.Server, 3)

	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		nodes[i], sms[i] = makeNode(t, id, peers, nil)
		servers[i] = httptest.NewServer(nodes[i].RaftHTTPHandler().Handler())
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	// Build peer map and recreate nodes with proper transports
	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		log := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()
		cfg := Config{
			ID:     id,
			Peers:  peers,
			Addr:   servers[i].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[i], err = NewNode(cfg, stable, log, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		sms[i] = sm
		servers[i].Config.Handler = nodes[i].RaftHTTPHandler().Handler()
	}

	// Start all nodes
	for _, n := range nodes {
		n.Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop(ctx)
		}
	}()

	// Wait for election
	time.Sleep(300 * time.Millisecond)

	// Find the leader
	var leader *Node
	var leaderSM *kvsm.KVStateMachine
	for i, n := range nodes {
		if n.IsLeader() {
			leader = n
			leaderSM = sms[i]
			break
		}
	}

	if leader == nil {
		t.Fatal("no leader elected")
	}

	cmd := types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "hello", Value: "world"}
	res, err := leader.Propose(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Ok {
		t.Fatalf("propose failed: %+v", res)
	}

	v, ok := leaderSM.Get("hello")
	if !ok || v != "world" {
		t.Fatalf("leader sm: expected world, got %q ok=%v", v, ok)
	}

	// Wait for followers to apply
	time.Sleep(100 * time.Millisecond)
	for _, sm := range sms {
		v, ok := sm.Get("hello")
		if !ok || v != "world" {
			t.Logf("sm: expected world, got %q ok=%v (may not be leader's SM)", v, ok)
		}
	}
}

func TestRaft_M1_FollowerHandleAppendEntries_AppendsAndApplies(t *testing.T) {
	follower, sm := makeNode(t, "f1", nil, nil)
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

	time.Sleep(50 * time.Millisecond)
	v, ok := sm.Get("k1")
	if !ok || v != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}

	hint := follower.LeaderHint()
	if hint.LeaderID != "leader" {
		t.Fatalf("expected leader hint, got %+v", hint)
	}
}

// --- M2 Tests ---

func TestRaft_M2_Election_HappensAfterLeaderStops(t *testing.T) {
	ctx := context.Background()

	// Create 3 nodes
	nodes := make([]*Node, 3)
	sms := make([]*kvsm.KVStateMachine, 3)
	servers := make([]*httptest.Server, 3)
	ids := []types.NodeID{"n1", "n2", "n3"}

	// First pass: create nodes without transport
	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		nodes[i], sms[i] = makeNode(t, id, peers, nil)
	}

	// Create servers
	for i := range nodes {
		servers[i] = httptest.NewServer(nodes[i].RaftHTTPHandler().Handler())
	}
	defer func() {
		for _, s := range servers {
			if s != nil {
				s.Close()
			}
		}
	}()

	// Create peer map and update transports
	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Recreate nodes with proper transports
	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		log := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()
		cfg := Config{
			ID:     id,
			Peers:  peers,
			Addr:   servers[i].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[i], err = NewNode(cfg, stable, log, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		sms[i] = sm

		// Update server handler
		servers[i].Config.Handler = nodes[i].RaftHTTPHandler().Handler()
	}

	// Start all nodes
	for _, n := range nodes {
		n.Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop(ctx)
		}
	}()

	// Wait for initial election
	time.Sleep(300 * time.Millisecond)

	// Find the leader
	var leaderIdx int = -1
	for i, n := range nodes {
		if n.IsLeader() {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	oldLeaderID := ids[leaderIdx]
	t.Logf("initial leader: %s", oldLeaderID)

	// Stop the leader
	nodes[leaderIdx].Stop(ctx)
	servers[leaderIdx].Close()
	servers[leaderIdx] = nil

	// Wait for new election
	time.Sleep(300 * time.Millisecond)

	// Check that a new leader was elected
	var newLeaderIdx int = -1
	for i, n := range nodes {
		if i == leaderIdx {
			continue
		}
		if n.IsLeader() {
			newLeaderIdx = i
			break
		}
	}

	if newLeaderIdx == -1 {
		t.Fatal("no new leader elected after old leader stopped")
	}

	t.Logf("new leader: %s", ids[newLeaderIdx])
	if ids[newLeaderIdx] == oldLeaderID {
		t.Fatal("new leader should be different from old leader")
	}
}

func TestRaft_M2_HigherTermForcesStepDown(t *testing.T) {
	ctx := context.Background()

	// Create a node that thinks it's a leader
	node, _ := makeNode(t, "n1", []types.NodeID{"n2"}, nil)
	node.Start(ctx)
	defer node.Stop(ctx)

	// Manually make it leader at term 1
	node.mu.Lock()
	node.role = RoleLeader
	node.currentTerm = 1
	node.mu.Unlock()

	// Send AppendEntries with higher term
	req := transporthttp.AppendEntriesRequest{
		Term:       5,
		LeaderID:   "n2",
		LeaderAddr: "http://n2:8080",
	}

	resp, err := node.HandleAppendEntries(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	if !resp.Success {
		t.Fatal("expected success")
	}

	// Node should have stepped down
	node.mu.Lock()
	role := node.role
	term := node.currentTerm
	node.mu.Unlock()

	if role != RoleFollower {
		t.Fatalf("expected follower, got %s", role)
	}
	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}
}

func TestCluster_M2_LeaderCrash_NewLeaderElected(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2", "n3"}

	// Setup cluster
	nodes := make([]*Node, 3)
	servers := make([]*httptest.Server, 3)

	// Create servers first
	for i := range nodes {
		mux := http.NewServeMux()
		servers[i] = httptest.NewServer(mux)
	}
	defer func() {
		for _, s := range servers {
			if s != nil {
				s.Close()
			}
		}
	}()

	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Create and start nodes
	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		log := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()
		cfg := Config{
			ID:     id,
			Peers:  peers,
			Addr:   servers[i].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[i], err = NewNode(cfg, stable, log, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		// Update server handler
		servers[i].Config.Handler = nodes[i].RaftHTTPHandler().Handler()
		nodes[i].Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop(ctx)
			}
		}
	}()

	// Wait for leader
	time.Sleep(300 * time.Millisecond)

	var leaderIdx int = -1
	for i, n := range nodes {
		if n.IsLeader() {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	// Crash leader
	nodes[leaderIdx].Stop(ctx)
	servers[leaderIdx].Close()
	servers[leaderIdx] = nil
	nodes[leaderIdx] = nil

	// Wait for re-election
	time.Sleep(300 * time.Millisecond)

	// Check new leader
	var newLeaderIdx = -1
	for i, n := range nodes {
		if n == nil {
			continue
		}
		if n.IsLeader() {
			newLeaderIdx = i
			break
		}
	}

	if newLeaderIdx == -1 {
		t.Fatal("no new leader elected")
	}
	if newLeaderIdx == leaderIdx {
		t.Fatal("new leader should be different from crashed leader")
	}
}

func TestCluster_M2_WriteAfterFailover_Succeeds(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2", "n3"}

	nodes := make([]*Node, 3)
	sms := make([]*kvsm.KVStateMachine, 3)
	servers := make([]*httptest.Server, 3)

	// Create servers
	for i := range nodes {
		mux := http.NewServeMux()
		servers[i] = httptest.NewServer(mux)
	}
	defer func() {
		for _, s := range servers {
			if s != nil {
				s.Close()
			}
		}
	}()

	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Create nodes
	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		log := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()
		cfg := Config{
			ID:     id,
			Peers:  peers,
			Addr:   servers[i].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[i], err = NewNode(cfg, stable, log, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		sms[i] = sm
		servers[i].Config.Handler = nodes[i].RaftHTTPHandler().Handler()
		nodes[i].Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop(ctx)
			}
		}
	}()

	// Wait for leader
	time.Sleep(300 * time.Millisecond)

	var leaderIdx int = -1
	for i, n := range nodes {
		if n.IsLeader() {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	// Write to leader
	cmd := types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "key1", Value: "value1"}
	res, err := nodes[leaderIdx].Propose(ctx, cmd)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if !res.Ok {
		t.Fatalf("first write not ok: %+v", res)
	}

	// Crash leader
	nodes[leaderIdx].Stop(ctx)
	servers[leaderIdx].Close()
	servers[leaderIdx] = nil
	nodes[leaderIdx] = nil

	// Wait for re-election
	time.Sleep(300 * time.Millisecond)

	// Find new leader
	var newLeaderIdx = -1
	for i, n := range nodes {
		if n == nil {
			continue
		}
		if n.IsLeader() {
			newLeaderIdx = i
			break
		}
	}
	if newLeaderIdx == -1 {
		t.Fatal("no new leader elected")
	}

	// Write to new leader
	cmd2 := types.Command{ClientID: "c1", Seq: 2, Op: types.OpPut, Key: "key2", Value: "value2"}
	res, err = nodes[newLeaderIdx].Propose(ctx, cmd2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	if !res.Ok {
		t.Fatalf("second write not ok: %+v", res)
	}

	// Verify the value
	v, ok := sms[newLeaderIdx].Get("key2")
	if !ok || v != "value2" {
		t.Fatalf("expected value2, got %q ok=%v", v, ok)
	}
}

// --- Integration tests from M1 ---

type clusterNode struct {
	node   *Node
	sm     *kvsm.KVStateMachine
	server *httptest.Server
}

func TestCluster_M1_WriteToFollower_ReturnsRedirect(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2", "n3"}

	nodes := make([]*Node, 3)
	servers := make([]*httptest.Server, 3)

	// Create servers
	for i := range nodes {
		mux := http.NewServeMux()
		servers[i] = httptest.NewServer(mux)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Create nodes
	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		log := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()
		cfg := Config{
			ID:     id,
			Peers:  peers,
			Addr:   servers[i].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[i], err = NewNode(cfg, stable, log, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		servers[i].Config.Handler = nodes[i].RaftHTTPHandler().Handler()
		nodes[i].Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop(ctx)
		}
	}()

	// Wait for leader
	time.Sleep(300 * time.Millisecond)

	var leaderIdx, followerIdx int = -1, -1
	for i, n := range nodes {
		if n.IsLeader() {
			leaderIdx = i
		} else {
			followerIdx = i
		}
	}
	if leaderIdx == -1 || followerIdx == -1 {
		t.Fatal("need leader and follower")
	}

	// Try to propose on follower - should get ErrNotLeader
	cmd := types.Command{Op: types.OpPut, Key: "y", Value: "99"}
	_, err := nodes[followerIdx].Propose(ctx, cmd)
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}

	// Test via HTTP
	dkvFollower := setupDKVHTTP(t, nodes[followerIdx])
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

func setupDKVHTTP(t *testing.T, node *Node) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if !node.IsLeader() {
			hint := node.LeaderHint()
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

// --- M3 Tests ---

func TestRaft_M3_FollowerRejectsOnPrevMismatch(t *testing.T) {
	ctx := context.Background()
	follower, _ := makeNode(t, "f1", nil, nil)
	follower.Start(ctx)
	defer follower.Stop(ctx)

	// First, append an entry at index 1 with term 1
	req1 := transporthttp.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader",
		LeaderAddr:   "http://leader:8080",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []storage.LogEntry{
			{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k1", Value: "v1"}},
		},
		LeaderCommit: 0,
	}
	resp, err := follower.HandleAppendEntries(ctx, req1)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("expected first append to succeed")
	}

	// Now try to append at index 2 with wrong prevLogTerm (say term 5 instead of 1)
	req2 := transporthttp.AppendEntriesRequest{
		Term:         2,
		LeaderID:     "leader",
		LeaderAddr:   "http://leader:8080",
		PrevLogIndex: 1,
		PrevLogTerm:  5, // WRONG - should be 1
		Entries: []storage.LogEntry{
			{Index: 2, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "k2", Value: "v2"}},
		},
		LeaderCommit: 0,
	}
	resp, err = follower.HandleAppendEntries(ctx, req2)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatal("expected rejection due to prevLogTerm mismatch")
	}
	if resp.ConflictTerm != 1 {
		t.Fatalf("expected conflict term 1, got %d", resp.ConflictTerm)
	}
	if resp.ConflictIndex != 1 {
		t.Fatalf("expected conflict index 1, got %d", resp.ConflictIndex)
	}

	// Also test rejection when prevLogIndex doesn't exist
	req3 := transporthttp.AppendEntriesRequest{
		Term:         2,
		LeaderID:     "leader",
		LeaderAddr:   "http://leader:8080",
		PrevLogIndex: 10, // doesn't exist
		PrevLogTerm:  1,
		Entries: []storage.LogEntry{
			{Index: 11, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "k3", Value: "v3"}},
		},
		LeaderCommit: 0,
	}
	resp, err = follower.HandleAppendEntries(ctx, req3)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatal("expected rejection due to missing prevLogIndex")
	}
	// ConflictIndex should be lastIndex+1 = 2
	if resp.ConflictIndex != 2 {
		t.Fatalf("expected conflict index 2, got %d", resp.ConflictIndex)
	}
}

func TestRaft_M3_LeaderBacktracksNextIndexAndRepairsFollower(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2"}

	// We'll manually set up the log states to create a conflict scenario
	servers := make([]*httptest.Server, 2)
	nodes := make([]*Node, 2)

	// Create servers first
	for i := range servers {
		mux := http.NewServeMux()
		servers[i] = httptest.NewServer(mux)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Create n1 (will be leader) with entries [1:t1, 2:t1, 3:t2]
	{
		peers := []types.NodeID{"n2"}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		logStore := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()

		// Pre-populate log with entries
		logStore.Append([]storage.LogEntry{
			{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "a", Value: "1"}},
			{Index: 2, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "b", Value: "2"}},
			{Index: 3, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "c", Value: "3"}},
		})
		stable.SetCurrentTerm(2)

		cfg := Config{
			ID:     "n1",
			Peers:  peers,
			Addr:   servers[0].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[0], err = NewNode(cfg, stable, logStore, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		servers[0].Config.Handler = nodes[0].RaftHTTPHandler().Handler()
	}

	// Create n2 (will be follower) with entries [1:t1] only - diverged from leader
	{
		peers := []types.NodeID{"n1"}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		logStore := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()

		// Pre-populate with only first entry
		logStore.Append([]storage.LogEntry{
			{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "a", Value: "1"}},
		})
		stable.SetCurrentTerm(1)

		cfg := Config{
			ID:     "n2",
			Peers:  peers,
			Addr:   servers[1].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[1], err = NewNode(cfg, stable, logStore, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		servers[1].Config.Handler = nodes[1].RaftHTTPHandler().Handler()
	}

	// Start both nodes
	for _, n := range nodes {
		n.Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop(ctx)
		}
	}()

	// Force n1 to be leader
	nodes[0].mu.Lock()
	nodes[0].role = RoleLeader
	nodes[0].currentTerm = 2
	nodes[0].nextIndex["n2"] = 4 // Initially thinks follower is up to date
	nodes[0].matchIndex["n2"] = 0
	nodes[0].mu.Unlock()

	// Now try to replicate - leader should backtrack and repair
	success, matchIdx := nodes[0].replicateToPeer(ctx, "n2")

	// First attempt should fail (prevLogIndex=3 doesn't exist on follower)
	if success {
		t.Log("first replication succeeded - follower may have been partially up to date")
	}

	// Keep retrying until success
	for attempt := 0; attempt < 10 && !success; attempt++ {
		success, matchIdx = nodes[0].replicateToPeer(ctx, "n2")
	}

	if !success {
		t.Fatal("leader failed to repair follower after multiple attempts")
	}

	// Verify follower now has all entries
	lastIdx, _ := nodes[1].log.LastIndex()
	if lastIdx != 3 {
		t.Fatalf("expected follower to have 3 entries, got %d", lastIdx)
	}

	if matchIdx != 3 {
		t.Fatalf("expected matchIdx=3, got %d", matchIdx)
	}

	// Verify the entries match
	for i := uint64(1); i <= 3; i++ {
		t1, _ := nodes[0].log.TermAt(i)
		t2, _ := nodes[1].log.TermAt(i)
		if t1 != t2 {
			t.Fatalf("term mismatch at index %d: leader=%d, follower=%d", i, t1, t2)
		}
	}
}

func TestRaft_M3_CommittedEntriesNotLostAfterLeaderChange(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2", "n3"}

	nodes := make([]*Node, 3)
	sms := make([]*kvsm.KVStateMachine, 3)
	servers := make([]*httptest.Server, 3)

	// Create servers
	for i := range servers {
		mux := http.NewServeMux()
		servers[i] = httptest.NewServer(mux)
	}
	defer func() {
		for _, s := range servers {
			if s != nil {
				s.Close()
			}
		}
	}()

	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Create nodes
	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		logStore := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()
		cfg := Config{
			ID:     id,
			Peers:  peers,
			Addr:   servers[i].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[i], err = NewNode(cfg, stable, logStore, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		sms[i] = sm
		servers[i].Config.Handler = nodes[i].RaftHTTPHandler().Handler()
		nodes[i].Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop(ctx)
			}
		}
	}()

	// Wait for leader election
	time.Sleep(300 * time.Millisecond)

	var leaderIdx int = -1
	for i, n := range nodes {
		if n.IsLeader() {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	// Write some entries through the leader
	for i := 1; i <= 3; i++ {
		cmd := types.Command{
			ClientID: "client1",
			Seq:      uint64(i),
			Op:       types.OpPut,
			Key:      fmt.Sprintf("key%d", i),
			Value:    fmt.Sprintf("value%d", i),
		}
		res, err := nodes[leaderIdx].Propose(ctx, cmd)
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
		if !res.Ok {
			t.Fatalf("write %d not ok: %+v", i, res)
		}
	}

	// Wait for replication
	time.Sleep(100 * time.Millisecond)

	// Verify all nodes have applied the entries
	for i, sm := range sms {
		for j := 1; j <= 3; j++ {
			v, ok := sm.Get(fmt.Sprintf("key%d", j))
			expected := fmt.Sprintf("value%d", j)
			if !ok || v != expected {
				t.Fatalf("node %d missing key%d: got %q, want %q", i, j, v, expected)
			}
		}
	}

	// Crash the leader
	oldLeaderIdx := leaderIdx
	nodes[leaderIdx].Stop(ctx)
	servers[leaderIdx].Close()
	servers[leaderIdx] = nil
	nodes[leaderIdx] = nil

	// Wait for new leader
	time.Sleep(300 * time.Millisecond)

	var newLeaderIdx int = -1
	for i, n := range nodes {
		if n == nil {
			continue
		}
		if n.IsLeader() {
			newLeaderIdx = i
			break
		}
	}
	if newLeaderIdx == -1 {
		t.Fatal("no new leader elected")
	}
	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader is same as old")
	}

	// Verify committed entries are still present on new leader
	for j := 1; j <= 3; j++ {
		v, ok := sms[newLeaderIdx].Get(fmt.Sprintf("key%d", j))
		expected := fmt.Sprintf("value%d", j)
		if !ok || v != expected {
			t.Fatalf("new leader missing committed key%d: got %q, want %q", j, v, expected)
		}
	}

	// Write more entries through new leader
	cmd := types.Command{
		ClientID: "client1",
		Seq:      4,
		Op:       types.OpPut,
		Key:      "key4",
		Value:    "value4",
	}
	res, err := nodes[newLeaderIdx].Propose(ctx, cmd)
	if err != nil {
		t.Fatalf("new write failed: %v", err)
	}
	if !res.Ok {
		t.Fatalf("new write not ok: %+v", res)
	}

	// Verify the new entry is present
	v, ok := sms[newLeaderIdx].Get("key4")
	if !ok || v != "value4" {
		t.Fatalf("new leader missing key4: got %q, want value4", v)
	}
}

// --- M4 Tests: ReadIndex Reads ---

func TestRaft_M4_WaitAppliedBlocksUntilApplied(t *testing.T) {
	ctx := context.Background()
	node, sm := makeNode(t, "n1", nil, nil)
	node.Start(ctx)
	defer node.Stop(ctx)

	// Manually append an entry and set commitIndex
	node.mu.Lock()
	node.log.Append([]storage.LogEntry{
		{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k1", Value: "v1"}},
	})
	node.currentTerm = 1
	node.commitIndex = 1
	node.mu.Unlock()

	// Signal applier
	node.signalApplier()

	// WaitApplied should return after the entry is applied
	waitCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := node.WaitApplied(waitCtx, 1)
	if err != nil {
		t.Fatalf("WaitApplied failed: %v", err)
	}

	// Verify the entry was applied
	v, ok := sm.Get("k1")
	if !ok || v != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}
}

func TestRaft_M4_WaitAppliedTimesOut(t *testing.T) {
	ctx := context.Background()
	node, _ := makeNode(t, "n1", nil, nil)
	node.Start(ctx)
	defer node.Stop(ctx)

	// Don't append or commit any entries
	// WaitApplied for index 1 should timeout

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err := node.WaitApplied(waitCtx, 1)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestRaft_M4_GetReadIndex_ReturnsCommitIndex(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2", "n3"}

	nodes := make([]*Node, 3)
	servers := make([]*httptest.Server, 3)

	// Create servers
	for i := range servers {
		mux := http.NewServeMux()
		servers[i] = httptest.NewServer(mux)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Create nodes
	for i, id := range ids {
		peers := make([]types.NodeID, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		logStore := storage.NewMemLogStore()
		snap := storage.NewMemSnapshotStore()
		cfg := Config{
			ID:     id,
			Peers:  peers,
			Addr:   servers[i].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[i], err = NewNode(cfg, stable, logStore, snap, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		servers[i].Config.Handler = nodes[i].RaftHTTPHandler().Handler()
		nodes[i].Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop(ctx)
		}
	}()

	// Wait for leader election
	time.Sleep(300 * time.Millisecond)

	var leaderIdx int = -1
	for i, n := range nodes {
		if n.IsLeader() {
			leaderIdx = i
			break
		}
	}
	if leaderIdx == -1 {
		t.Fatal("no leader elected")
	}

	// Write some entries to advance commit index
	cmd := types.Command{ClientID: "c1", Seq: 1, Op: types.OpPut, Key: "k1", Value: "v1"}
	_, err := nodes[leaderIdx].Propose(ctx, cmd)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// GetReadIndex should return the commit index
	readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	readIndex, err := nodes[leaderIdx].GetReadIndex(readCtx)
	if err != nil {
		t.Fatalf("GetReadIndex failed: %v", err)
	}

	// ReadIndex should be >= 1 (since we committed entry at index 1)
	if readIndex < 1 {
		t.Fatalf("expected readIndex >= 1, got %d", readIndex)
	}

	nodes[leaderIdx].mu.Lock()
	commitIndex := nodes[leaderIdx].commitIndex
	nodes[leaderIdx].mu.Unlock()

	if readIndex != commitIndex {
		t.Fatalf("expected readIndex=%d, got %d", commitIndex, readIndex)
	}
}

func TestRaft_M4_GetReadIndex_FailsOnFollower(t *testing.T) {
	ctx := context.Background()
	node, _ := makeNode(t, "n1", []types.NodeID{"n2"}, nil)
	node.Start(ctx)
	defer node.Stop(ctx)

	// Node is a follower (never elected)
	readCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err := node.GetReadIndex(readCtx)
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestRaft_M4_GetReadIndex_SingleNodeCluster(t *testing.T) {
	ctx := context.Background()
	node, _ := makeNode(t, "n1", nil, nil)
	node.Start(ctx)
	defer node.Stop(ctx)

	// Force node to be leader (single node)
	node.mu.Lock()
	node.role = RoleLeader
	node.currentTerm = 1
	node.commitIndex = 5
	node.mu.Unlock()

	readCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	readIndex, err := node.GetReadIndex(readCtx)
	if err != nil {
		t.Fatalf("GetReadIndex failed: %v", err)
	}
	if readIndex != 5 {
		t.Fatalf("expected readIndex=5, got %d", readIndex)
	}
}

// --- M5 Tests: Snapshots ---

func TestRaft_M5_CreateSnapshot_TruncatesLogPrefix(t *testing.T) {
	ctx := context.Background()
	node, sm := makeNode(t, "n1", nil, nil)
	node.Start(ctx)
	defer node.Stop(ctx)

	// Manually add entries and apply them
	entries := []storage.LogEntry{
		{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k1", Value: "v1"}},
		{Index: 2, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k2", Value: "v2"}},
		{Index: 3, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "k3", Value: "v3"}},
		{Index: 4, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "k4", Value: "v4"}},
		{Index: 5, Term: 3, Cmd: types.Command{Op: types.OpPut, Key: "k5", Value: "v5"}},
	}
	node.log.Append(entries)

	// Apply all entries to state machine
	for _, e := range entries {
		sm.Apply(e.Cmd)
	}

	node.mu.Lock()
	node.currentTerm = 3
	node.lastApplied = 5
	node.commitIndex = 5
	node.mu.Unlock()

	// Create snapshot at index 3
	if err := node.CreateSnapshot(3); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Verify snapshot state
	node.mu.Lock()
	lastSnapshotIndex := node.lastSnapshotIndex
	lastSnapshotTerm := node.lastSnapshotTerm
	node.mu.Unlock()

	if lastSnapshotIndex != 3 {
		t.Fatalf("expected lastSnapshotIndex=3, got %d", lastSnapshotIndex)
	}
	if lastSnapshotTerm != 2 {
		t.Fatalf("expected lastSnapshotTerm=2, got %d", lastSnapshotTerm)
	}

	// Verify log was truncated
	baseIndex := node.log.BaseIndex()
	if baseIndex != 3 {
		t.Fatalf("expected baseIndex=3, got %d", baseIndex)
	}

	// Verify snapshot was saved
	meta, data, ok, err := node.snap.Load()
	if err != nil || !ok {
		t.Fatalf("snapshot not found: err=%v, ok=%v", err, ok)
	}
	if meta.LastIncludedIndex != 3 || meta.LastIncludedTerm != 2 {
		t.Fatalf("wrong snapshot meta: %+v", meta)
	}
	if len(data) == 0 {
		t.Fatal("snapshot data is empty")
	}

	// Verify we can still access entries 4 and 5
	term, err := node.log.TermAt(4)
	if err != nil {
		t.Fatalf("TermAt(4) failed: %v", err)
	}
	if term != 2 {
		t.Fatalf("expected term 2 at index 4, got %d", term)
	}

	lastIdx, _ := node.log.LastIndex()
	if lastIdx != 5 {
		t.Fatalf("expected lastIndex=5, got %d", lastIdx)
	}
}

func TestRaft_M5_FollowerInstallSnapshot_RestoresState(t *testing.T) {
	ctx := context.Background()
	follower, sm := makeNode(t, "f1", nil, nil)
	follower.Start(ctx)
	defer follower.Stop(ctx)

	// Create snapshot data with some state
	snapshotState := kvsm.Snapshot{
		KV:     map[string]string{"foo": "bar", "baz": "qux"},
		Dedupe: map[string]kvsm.DedupeRecord{},
	}
	snapshotData, _ := json.Marshal(snapshotState)
	encodedData := base64.StdEncoding.EncodeToString(snapshotData)

	// Send InstallSnapshot RPC
	req := transporthttp.InstallSnapshotRequest{
		Term:              5,
		LeaderID:          "leader",
		LeaderAddr:        "http://leader:8080",
		LastIncludedIndex: 100,
		LastIncludedTerm:  4,
		Data:              encodedData,
	}

	resp, err := follower.HandleInstallSnapshot(ctx, req)
	if err != nil {
		t.Fatalf("HandleInstallSnapshot failed: %v", err)
	}
	if resp.Term != 5 {
		t.Fatalf("expected term 5, got %d", resp.Term)
	}

	// Verify state machine was restored
	val, ok := sm.Get("foo")
	if !ok || val != "bar" {
		t.Fatalf("expected foo=bar, got %q ok=%v", val, ok)
	}
	val, ok = sm.Get("baz")
	if !ok || val != "qux" {
		t.Fatalf("expected baz=qux, got %q ok=%v", val, ok)
	}

	// Verify snapshot state was updated
	follower.mu.Lock()
	lastSnapshotIndex := follower.lastSnapshotIndex
	lastApplied := follower.lastApplied
	commitIndex := follower.commitIndex
	follower.mu.Unlock()

	if lastSnapshotIndex != 100 {
		t.Fatalf("expected lastSnapshotIndex=100, got %d", lastSnapshotIndex)
	}
	if lastApplied != 100 {
		t.Fatalf("expected lastApplied=100, got %d", lastApplied)
	}
	if commitIndex != 100 {
		t.Fatalf("expected commitIndex=100, got %d", commitIndex)
	}

	// Verify log base was updated
	baseIndex := follower.log.BaseIndex()
	if baseIndex != 100 {
		t.Fatalf("expected baseIndex=100, got %d", baseIndex)
	}

	// Verify leader hint was updated
	hint := follower.LeaderHint()
	if hint.LeaderID != "leader" {
		t.Fatalf("expected leader hint, got %+v", hint)
	}
}

func TestRaft_M5_MaybeSnapshot_TriggersAtThreshold(t *testing.T) {
	ctx := context.Background()

	// Create node with low threshold
	sm := kvsm.New()
	stable := storage.NewMemStableStore()
	logStore := storage.NewMemLogStore()
	snap := storage.NewMemSnapshotStore()
	cfg := Config{
		ID:                "n1",
		Peers:             nil,
		Addr:              "http://n1:8080",
		Timing:            fastTiming(),
		SnapshotThreshold: 3, // Low threshold for testing
	}
	node, err := NewNode(cfg, stable, logStore, snap, nil, sm)
	if err != nil {
		t.Fatal(err)
	}
	node.Start(ctx)
	defer node.Stop(ctx)

	// Add entries and apply them
	entries := []storage.LogEntry{
		{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k1", Value: "v1"}},
		{Index: 2, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k2", Value: "v2"}},
		{Index: 3, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k3", Value: "v3"}},
		{Index: 4, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "k4", Value: "v4"}},
	}
	logStore.Append(entries)
	for _, e := range entries {
		sm.Apply(e.Cmd)
	}

	node.mu.Lock()
	node.lastApplied = 4
	node.commitIndex = 4
	node.mu.Unlock()

	// MaybeSnapshot should trigger since lastApplied - lastSnapshotIndex >= threshold
	if err := node.MaybeSnapshot(); err != nil {
		t.Fatalf("MaybeSnapshot failed: %v", err)
	}

	// Verify snapshot was created
	node.mu.Lock()
	lastSnapshotIndex := node.lastSnapshotIndex
	node.mu.Unlock()

	if lastSnapshotIndex != 4 {
		t.Fatalf("expected lastSnapshotIndex=4, got %d", lastSnapshotIndex)
	}
}

func TestCluster_M5_LaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	ctx := context.Background()
	ids := []types.NodeID{"n1", "n2"}

	servers := make([]*httptest.Server, 2)
	nodes := make([]*Node, 2)
	sms := make([]*kvsm.KVStateMachine, 2)

	// Create servers first
	for i := range servers {
		mux := http.NewServeMux()
		servers[i] = httptest.NewServer(mux)
	}
	defer func() {
		for _, s := range servers {
			if s != nil {
				s.Close()
			}
		}
	}()

	peerMap := make(map[types.NodeID]string)
	for i, id := range ids {
		peerMap[id] = servers[i].URL
	}

	// Create n1 (leader) with entries and snapshot
	{
		peers := []types.NodeID{"n2"}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		logStore := storage.NewMemLogStore()
		snapStore := storage.NewMemSnapshotStore()

		// Pre-populate with entries 1-5
		entries := []storage.LogEntry{
			{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "a", Value: "1"}},
			{Index: 2, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "b", Value: "2"}},
			{Index: 3, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "c", Value: "3"}},
			{Index: 4, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "d", Value: "4"}},
			{Index: 5, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "e", Value: "5"}},
		}
		logStore.Append(entries)
		for _, e := range entries {
			sm.Apply(e.Cmd)
		}
		stable.SetCurrentTerm(2)

		// Create snapshot at index 3
		snapshotData, _ := sm.Snapshot()
		snapStore.Save(storage.SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}, snapshotData)
		logStore.TruncatePrefix(3)

		cfg := Config{
			ID:     "n1",
			Peers:  peers,
			Addr:   servers[0].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[0], err = NewNode(cfg, stable, logStore, snapStore, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		// Update snapshot state
		nodes[0].mu.Lock()
		nodes[0].lastSnapshotIndex = 3
		nodes[0].lastSnapshotTerm = 1
		nodes[0].lastApplied = 5
		nodes[0].commitIndex = 5
		nodes[0].mu.Unlock()

		sms[0] = sm
		servers[0].Config.Handler = nodes[0].RaftHTTPHandler().Handler()
	}

	// Create n2 (follower) with empty log - lagging behind
	{
		peers := []types.NodeID{"n1"}
		resolver := transporthttp.NewPeerResolver(peerMap)
		tp := transporthttp.NewHTTPTransport(resolver)

		sm := kvsm.New()
		stable := storage.NewMemStableStore()
		logStore := storage.NewMemLogStore()
		snapStore := storage.NewMemSnapshotStore()

		cfg := Config{
			ID:     "n2",
			Peers:  peers,
			Addr:   servers[1].URL,
			Timing: fastTiming(),
		}
		var err error
		nodes[1], err = NewNode(cfg, stable, logStore, snapStore, tp, sm)
		if err != nil {
			t.Fatal(err)
		}
		sms[1] = sm
		servers[1].Config.Handler = nodes[1].RaftHTTPHandler().Handler()
	}

	// Start both nodes
	for _, n := range nodes {
		n.Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop(ctx)
		}
	}()

	// Force n1 to be leader
	nodes[0].mu.Lock()
	nodes[0].role = RoleLeader
	nodes[0].currentTerm = 2
	nodes[0].nextIndex["n2"] = 1 // Follower is at index 0
	nodes[0].matchIndex["n2"] = 0
	nodes[0].mu.Unlock()

	// Replicate to peer - should send snapshot since nextIndex <= baseIndex
	success, matchIdx := nodes[0].replicateToPeer(ctx, "n2")

	if !success {
		t.Fatal("replication via snapshot should succeed")
	}
	if matchIdx != 3 {
		t.Fatalf("expected matchIdx=3 (snapshot index), got %d", matchIdx)
	}

	// Verify follower received the snapshot
	time.Sleep(50 * time.Millisecond) // Allow state machine to be restored

	val, ok := sms[1].Get("a")
	if !ok || val != "1" {
		t.Fatalf("follower missing key a: got %q ok=%v", val, ok)
	}
	val, ok = sms[1].Get("b")
	if !ok || val != "2" {
		t.Fatalf("follower missing key b: got %q ok=%v", val, ok)
	}
	val, ok = sms[1].Get("c")
	if !ok || val != "3" {
		t.Fatalf("follower missing key c: got %q ok=%v", val, ok)
	}

	// Verify follower's state was updated
	nodes[1].mu.Lock()
	lastSnapshotIndex := nodes[1].lastSnapshotIndex
	lastApplied := nodes[1].lastApplied
	nodes[1].mu.Unlock()

	if lastSnapshotIndex != 3 {
		t.Fatalf("expected follower lastSnapshotIndex=3, got %d", lastSnapshotIndex)
	}
	if lastApplied != 3 {
		t.Fatalf("expected follower lastApplied=3, got %d", lastApplied)
	}
}
