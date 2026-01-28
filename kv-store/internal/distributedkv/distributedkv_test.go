package distributedkv

import (
	"context"
	"testing"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

type mockRaftNode struct {
	leader     bool
	leaderHint types.LeaderHint
	lastCmd    types.Command
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

	// Reads come from local SM regardless of leader status
	v, ok := dkv.Get("k1")
	if !ok || v != "v1" {
		t.Fatalf("get: expected v1, got %q", v)
	}

	vals := dkv.MGet([]string{"k1", "k2", "missing"})
	if vals["k1"] != "v1" || vals["k2"] != "v2" {
		t.Fatalf("mget mismatch: %v", vals)
	}
	if _, exists := vals["missing"]; exists {
		t.Fatal("missing key should not be in mget result")
	}
}
