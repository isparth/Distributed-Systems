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
	// M4: ReadIndex reads
	GetReadIndex(ctx context.Context) (uint64, error)
	WaitApplied(ctx context.Context, index uint64) error
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

// --- Reads ---

// Get retrieves a value. Behavior depends on ReadPolicy:
// - ReadPolicyStale: Returns immediately from local state machine (may be stale)
// - ReadPolicyReadIndex: Confirms leadership and waits for commit index to be applied (consistent)
func (d *DistributedKV) Get(ctx context.Context, key string) (string, bool, error) {
	if d.cfg.ReadPolicy == types.ReadPolicyReadIndex {
		if err := d.waitForReadIndex(ctx); err != nil {
			return "", false, err
		}
	}
	val, ok := d.sm.Get(key)
	return val, ok, nil
}

// MGet retrieves multiple values. Behavior depends on ReadPolicy.
func (d *DistributedKV) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	if d.cfg.ReadPolicy == types.ReadPolicyReadIndex {
		if err := d.waitForReadIndex(ctx); err != nil {
			return nil, err
		}
	}
	return d.sm.MGet(keys), nil
}

// GetStale always reads from local state machine (ignores ReadPolicy).
func (d *DistributedKV) GetStale(key string) (string, bool) {
	return d.sm.Get(key)
}

// MGetStale always reads from local state machine (ignores ReadPolicy).
func (d *DistributedKV) MGetStale(keys []string) map[string]string {
	return d.sm.MGet(keys)
}

// waitForReadIndex gets the read index from leader and waits for it to be applied.
func (d *DistributedKV) waitForReadIndex(ctx context.Context) error {
	readIndex, err := d.node.GetReadIndex(ctx)
	if err != nil {
		return err
	}
	return d.node.WaitApplied(ctx, readIndex)
}

// SetReadPolicy changes the read policy at runtime.
func (d *DistributedKV) SetReadPolicy(policy types.ReadPolicy) {
	d.cfg.ReadPolicy = policy
}

// GetReadPolicy returns the current read policy.
func (d *DistributedKV) GetReadPolicy() types.ReadPolicy {
	return d.cfg.ReadPolicy
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
