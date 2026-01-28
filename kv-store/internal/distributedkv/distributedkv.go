package distributedkv

import (
	"context"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// RaftNodeIface is the subset of raft.Node that DistributedKV needs.
type RaftNodeIface interface {
	Propose(ctx context.Context, cmd types.Command) (types.ApplyResult, error)
	IsLeader() bool
	LeaderHint() types.LeaderHint
	Status() types.NodeStatus
}

// Config configures the DistributedKV layer.
type Config struct {
	ReadPolicy           types.ReadPolicy
	WriteMode            types.WriteMode
	AsyncResultTTLSeconds int
}

// DistributedKV wraps Raft + KVSM into a single API for the HTTP layer.
type DistributedKV struct {
	node RaftNodeIface
	sm   *kvsm.KVStateMachine
	cfg  Config
}

// New creates a new DistributedKV.
func New(node RaftNodeIface, sm *kvsm.KVStateMachine, cfg Config) *DistributedKV {
	return &DistributedKV{node: node, sm: sm, cfg: cfg}
}

func (d *DistributedKV) IsLeader() bool {
	return d.node.IsLeader()
}

func (d *DistributedKV) LeaderHint() types.LeaderHint {
	return d.node.LeaderHint()
}

func (d *DistributedKV) Status() types.NodeStatus {
	return d.node.Status()
}

func (d *DistributedKV) All() map[string]string {
	return d.sm.All()
}

// --- Reads (stale, from local SM) ---

func (d *DistributedKV) Get(key string) (string, bool) {
	return d.sm.Get(key)
}

func (d *DistributedKV) MGet(keys []string) map[string]string {
	return d.sm.MGet(keys)
}

// --- Writes (sync, through Raft) ---

func (d *DistributedKV) Put(ctx context.Context, cmd types.Command) (types.ApplyResult, error) {
	cmd.Op = types.OpPut
	return d.node.Propose(ctx, cmd)
}

func (d *DistributedKV) Delete(ctx context.Context, cmd types.Command) (types.ApplyResult, error) {
	cmd.Op = types.OpDelete
	return d.node.Propose(ctx, cmd)
}

func (d *DistributedKV) CAS(ctx context.Context, cmd types.Command) (types.ApplyResult, error) {
	cmd.Op = types.OpCAS
	return d.node.Propose(ctx, cmd)
}

func (d *DistributedKV) MPut(ctx context.Context, cmd types.Command) (types.ApplyResult, error) {
	cmd.Op = types.OpBatchPut
	return d.node.Propose(ctx, cmd)
}

func (d *DistributedKV) MDelete(ctx context.Context, cmd types.Command) (types.ApplyResult, error) {
	cmd.Op = types.OpBatchDelete
	return d.node.Propose(ctx, cmd)
}
