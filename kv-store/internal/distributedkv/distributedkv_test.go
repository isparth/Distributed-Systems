package distributedkv

import (
	"context"
	"errors"
	"testing"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

var errNotLeader = errors.New("not leader")

type mockRaftNode struct {
	leader     bool
	leaderHint types.LeaderHint
	lastCmd    types.Command
	// M4: for testing ReadIndex reads
	readIndexErr   error
	waitAppliedErr error
	commitIndex    uint64
}

func (m *mockRaftNode) Propose(_ context.Context, cmd types.Command) (types.ApplyResult, error) {
	m.lastCmd = cmd
	return types.ApplyResult{Ok: true}, nil
}

func (m *mockRaftNode) IsLeader() bool {
	return m.leader
}

func (m *mockRaftNode) LeaderHint() types.LeaderHint {
	return m.leaderHint
}

func (m *mockRaftNode) Status() types.NodeStatus {
	return types.NodeStatus{
		ID:         "mock",
		Role:       "leader",
		LeaderHint: m.leaderHint,
	}
}

func (m *mockRaftNode) GetReadIndex(_ context.Context) (uint64, error) {
	if m.readIndexErr != nil {
		return 0, m.readIndexErr
	}
	return m.commitIndex, nil
}

func (m *mockRaftNode) WaitApplied(_ context.Context, index uint64) error {
	return m.waitAppliedErr
}

func TestDKV_M1_WritesCallPropose(t *testing.T) {
	sm := kvsm.New()
	node := &mockRaftNode{leader: true}
	dkv := New(node, sm, Config{})

	ctx := context.Background()

	// Put
	res, err := dkv.Put(ctx, types.Command{Key: "k", Value: "v"})
	if err != nil || !res.Ok {
		t.Fatalf("put: err=%v res=%+v", err, res)
	}
	if node.lastCmd.Op != types.OpPut {
		t.Fatalf("expected OpPut, got %v", node.lastCmd.Op)
	}

	// Delete
	res, err = dkv.Delete(ctx, types.Command{Key: "k"})
	if err != nil || !res.Ok {
		t.Fatalf("delete: err=%v", err)
	}
	if node.lastCmd.Op != types.OpDelete {
		t.Fatalf("expected OpDelete, got %v", node.lastCmd.Op)
	}

	// CAS
	res, err = dkv.CAS(ctx, types.Command{Key: "k", Expected: "", Value: "v2"})
	if err != nil || !res.Ok {
		t.Fatalf("cas: err=%v", err)
	}
	if node.lastCmd.Op != types.OpCAS {
		t.Fatalf("expected OpCAS, got %v", node.lastCmd.Op)
	}

	// MPut
	res, err = dkv.MPut(ctx, types.Command{Entries: []types.Entry{{Key: "a", Value: "1"}}})
	if err != nil || !res.Ok {
		t.Fatalf("mput: err=%v", err)
	}
	if node.lastCmd.Op != types.OpBatchPut {
		t.Fatalf("expected OpBatchPut, got %v", node.lastCmd.Op)
	}

	// MDelete
	res, err = dkv.MDelete(ctx, types.Command{Keys: []string{"a"}})
	if err != nil || !res.Ok {
		t.Fatalf("mdelete: err=%v", err)
	}
	if node.lastCmd.Op != types.OpBatchDelete {
		t.Fatalf("expected OpBatchDelete, got %v", node.lastCmd.Op)
	}
}

func TestDKV_M1_ReadsAreStaleFromSM(t *testing.T) {
	sm := kvsm.New()
	// Write directly to SM (simulating applied raft log)
	sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})
	sm.Apply(types.Command{Op: types.OpPut, Key: "k2", Value: "v2"})

	node := &mockRaftNode{leader: false}
	dkv := New(node, sm, Config{})

	// Reads come from local SM regardless of leader status (stale reads)
	v, ok := dkv.GetStale("k1")
	if !ok || v != "v1" {
		t.Fatalf("get: expected v1, got %q", v)
	}

	vals := dkv.MGetStale([]string{"k1", "k2", "missing"})
	if vals["k1"] != "v1" || vals["k2"] != "v2" {
		t.Fatalf("mget mismatch: %v", vals)
	}
	if _, exists := vals["missing"]; exists {
		t.Fatal("missing key should not be in mget result")
	}
}

// --- M4 Tests: ReadIndex Reads ---

func TestDKV_M4_ReadPolicyStale_ReadsImmediately(t *testing.T) {
	sm := kvsm.New()
	sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})

	node := &mockRaftNode{leader: true, commitIndex: 5}
	dkv := New(node, sm, Config{ReadPolicy: types.ReadPolicyStale})

	ctx := context.Background()
	v, ok, err := dkv.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || v != "v1" {
		t.Fatalf("expected v1, got %q", v)
	}
}

func TestDKV_M4_ReadPolicyReadIndex_WaitsForApply(t *testing.T) {
	sm := kvsm.New()
	sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})

	node := &mockRaftNode{leader: true, commitIndex: 5}
	dkv := New(node, sm, Config{ReadPolicy: types.ReadPolicyReadIndex})

	ctx := context.Background()
	v, ok, err := dkv.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || v != "v1" {
		t.Fatalf("expected v1, got %q", v)
	}
}

func TestDKV_M4_ReadPolicyReadIndex_FailsWhenNotLeader(t *testing.T) {
	sm := kvsm.New()
	sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})

	node := &mockRaftNode{leader: false, readIndexErr: errNotLeader}
	dkv := New(node, sm, Config{ReadPolicy: types.ReadPolicyReadIndex})

	ctx := context.Background()
	_, _, err := dkv.Get(ctx, "k1")
	if err == nil {
		t.Fatal("expected error for ReadIndex on non-leader")
	}
}

func TestDKV_M4_ReadPolicySwitch(t *testing.T) {
	sm := kvsm.New()
	sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})

	node := &mockRaftNode{leader: true, commitIndex: 5}
	dkv := New(node, sm, Config{ReadPolicy: types.ReadPolicyStale})

	ctx := context.Background()

	// Start with stale reads
	if dkv.GetReadPolicy() != types.ReadPolicyStale {
		t.Fatal("expected stale policy")
	}

	// Read works
	v, ok, err := dkv.Get(ctx, "k1")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("stale read failed: err=%v ok=%v v=%q", err, ok, v)
	}

	// Switch to ReadIndex
	dkv.SetReadPolicy(types.ReadPolicyReadIndex)
	if dkv.GetReadPolicy() != types.ReadPolicyReadIndex {
		t.Fatal("expected read_index policy")
	}

	// Read still works (mock returns immediately)
	v, ok, err = dkv.Get(ctx, "k1")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("read_index read failed: err=%v ok=%v v=%q", err, ok, v)
	}

	// Switch back to stale
	dkv.SetReadPolicy(types.ReadPolicyStale)
	if dkv.GetReadPolicy() != types.ReadPolicyStale {
		t.Fatal("expected stale policy after switch back")
	}
}

func TestDKV_M4_MGetWithReadIndex(t *testing.T) {
	sm := kvsm.New()
	sm.Apply(types.Command{Op: types.OpPut, Key: "k1", Value: "v1"})
	sm.Apply(types.Command{Op: types.OpPut, Key: "k2", Value: "v2"})

	node := &mockRaftNode{leader: true, commitIndex: 5}
	dkv := New(node, sm, Config{ReadPolicy: types.ReadPolicyReadIndex})

	ctx := context.Background()
	vals, err := dkv.MGet(ctx, []string{"k1", "k2", "missing"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vals["k1"] != "v1" || vals["k2"] != "v2" {
		t.Fatalf("mget mismatch: %v", vals)
	}
	if _, exists := vals["missing"]; exists {
		t.Fatal("missing key should not be in result")
	}
}
