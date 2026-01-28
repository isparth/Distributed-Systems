package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/storage"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/transporthttp"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

const (
	RoleLeader   = "leader"
	RoleFollower = "follower"
)

var ErrNotLeader = errors.New("not leader")

// Config holds configuration for a Raft node.
type Config struct {
	ID    types.NodeID
	Peers []types.NodeID // other nodes (not including self)
	Addr  string         // this node's advertised address
	Role  string         // initial role: "leader" or "follower" (fixed in M1)
}

// Status exposes internal state for debugging.
type Status struct {
	ID          types.NodeID   `json:"id"`
	Role        string         `json:"role"`
	Term        uint64         `json:"term"`
	CommitIndex uint64         `json:"commit_index"`
	LastApplied uint64         `json:"last_applied"`
	LastIndex   uint64         `json:"last_index"`
	LeaderHint  types.LeaderHint `json:"leader_hint"`
}

// pendingProposal tracks a proposal waiting to be applied.
type pendingProposal struct {
	index  uint64
	result chan types.ApplyResult
}

// Node is a Raft node.
type Node struct {
	cfg    Config
	stable storage.StableStore
	log    storage.LogStore
	snap   storage.SnapshotStore
	tp     transporthttp.Transport
	sm     *kvsm.KVStateMachine

	mu          sync.Mutex
	role        string
	currentTerm uint64
	leaderHint  types.LeaderHint
	commitIndex uint64
	lastApplied uint64

	matchIndex map[types.NodeID]uint64
	nextIndex  map[types.NodeID]uint64

	// applier
	applierDone chan struct{}
	applierCh   chan struct{} // signal new commits
	cancel      context.CancelFunc
	ctx         context.Context

	// pending proposals waiting for apply
	pendingMu sync.Mutex
	pending   map[uint64]chan types.ApplyResult
}

// NewNode creates a new Raft node.
func NewNode(cfg Config, stable storage.StableStore, log storage.LogStore, snap storage.SnapshotStore, tp transporthttp.Transport, sm *kvsm.KVStateMachine) (*Node, error) {
	term, err := stable.GetCurrentTerm()
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:         cfg,
		stable:      stable,
		log:         log,
		snap:        snap,
		tp:          tp,
		sm:          sm,
		role:        cfg.Role,
		currentTerm: term,
		matchIndex:  make(map[types.NodeID]uint64),
		nextIndex:   make(map[types.NodeID]uint64),
		applierCh:   make(chan struct{}, 1),
		pending:     make(map[uint64]chan types.ApplyResult),
	}

	if n.role == RoleLeader {
		n.leaderHint = types.LeaderHint{LeaderID: cfg.ID, LeaderAddr: cfg.Addr}
		lastIdx, _ := log.LastIndex()
		for _, p := range cfg.Peers {
			n.nextIndex[p] = lastIdx + 1
			n.matchIndex[p] = 0
		}
	}

	return n, nil
}

// Start starts the applier loop.
func (n *Node) Start(ctx context.Context) error {
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.applierDone = make(chan struct{})
	go n.applierLoop()
	return nil
}

// Stop shuts down the node.
func (n *Node) Stop(ctx context.Context) error {
	n.cancel()
	<-n.applierDone
	return nil
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

func (n *Node) LeaderHint() types.LeaderHint {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderHint
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	lastIdx, _ := n.log.LastIndex()
	return Status{
		ID:          n.cfg.ID,
		Role:        n.role,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LastIndex:   lastIdx,
		LeaderHint:  n.leaderHint,
	}
}

// Propose proposes a command. Only valid on the leader.
func (n *Node) Propose(ctx context.Context, cmd types.Command) (types.ApplyResult, error) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return types.ApplyResult{}, ErrNotLeader
	}
	term := n.currentTerm

	lastIdx, err := n.log.LastIndex()
	if err != nil {
		n.mu.Unlock()
		return types.ApplyResult{}, err
	}

	newIdx := lastIdx + 1
	entry := storage.LogEntry{Index: newIdx, Term: term, Cmd: cmd}
	if err := n.log.Append([]storage.LogEntry{entry}); err != nil {
		n.mu.Unlock()
		return types.ApplyResult{}, err
	}

	// Get prevLog info for AppendEntries
	var prevLogTerm uint64
	if lastIdx > 0 {
		prevLogTerm, _ = n.log.TermAt(lastIdx)
	}

	peers := make([]types.NodeID, len(n.cfg.Peers))
	copy(peers, n.cfg.Peers)
	commitIndex := n.commitIndex
	n.mu.Unlock()

	// Register pending proposal
	resultCh := make(chan types.ApplyResult, 1)
	n.pendingMu.Lock()
	n.pending[newIdx] = resultCh
	n.pendingMu.Unlock()
	defer func() {
		n.pendingMu.Lock()
		delete(n.pending, newIdx)
		n.pendingMu.Unlock()
	}()

	// Send AppendEntries to all peers
	req := transporthttp.AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.cfg.ID,
		LeaderAddr:   n.cfg.Addr,
		PrevLogIndex: lastIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      []storage.LogEntry{entry},
		LeaderCommit: commitIndex,
	}

	successCount := 1 // count self
	majority := (len(peers)+1)/2 + 1

	type peerResult struct {
		resp transporthttp.AppendEntriesResponse
		err  error
	}
	results := make(chan peerResult, len(peers))

	for _, p := range peers {
		go func(peer types.NodeID) {
			resp, err := n.tp.AppendEntries(ctx, peer, req)
			results <- peerResult{resp, err}
		}(p)
	}

	for range peers {
		pr := <-results
		if pr.err == nil && pr.resp.Success {
			successCount++
		}
	}

	if successCount < majority {
		return types.ApplyResult{}, fmt.Errorf("failed to replicate to majority: %d/%d", successCount, majority)
	}

	// Advance commitIndex
	n.mu.Lock()
	if newIdx > n.commitIndex {
		n.commitIndex = newIdx
	}
	n.mu.Unlock()

	// Signal applier
	n.signalApplier()

	// Notify followers of new commitIndex (empty AppendEntries as heartbeat)
	commitNotify := transporthttp.AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.cfg.ID,
		LeaderAddr:   n.cfg.Addr,
		PrevLogIndex: newIdx,
		PrevLogTerm:  term,
		LeaderCommit: newIdx,
	}
	for _, p := range peers {
		go n.tp.AppendEntries(ctx, p, commitNotify)
	}

	// Wait for apply
	select {
	case res := <-resultCh:
		return res, nil
	case <-ctx.Done():
		return types.ApplyResult{}, ctx.Err()
	}
}

// HandleAppendEntries handles an incoming AppendEntries RPC (follower side).
func (n *Node) HandleAppendEntries(ctx context.Context, req transporthttp.AppendEntriesRequest) (transporthttp.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Update leader hint
	n.leaderHint = types.LeaderHint{LeaderID: req.LeaderID, LeaderAddr: req.LeaderAddr}

	// Update term if needed
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.stable.SetCurrentTerm(req.Term)
	}

	// Append entries (no conflict checking in M1)
	if len(req.Entries) > 0 {
		if err := n.log.Append(req.Entries); err != nil {
			return transporthttp.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
		}
	}

	// Update commitIndex
	lastIdx, _ := n.log.LastIndex()
	newCommit := req.LeaderCommit
	if lastIdx < newCommit {
		newCommit = lastIdx
	}
	if newCommit > n.commitIndex {
		n.commitIndex = newCommit
	}

	n.signalApplierLocked()

	return transporthttp.AppendEntriesResponse{Term: n.currentTerm, Success: true}, nil
}

func (n *Node) signalApplier() {
	select {
	case n.applierCh <- struct{}{}:
	default:
	}
}

func (n *Node) signalApplierLocked() {
	select {
	case n.applierCh <- struct{}{}:
	default:
	}
}

func (n *Node) applierLoop() {
	defer close(n.applierDone)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.applierCh:
			n.applyPending()
		}
	}
}

func (n *Node) applyPending() {
	for {
		n.mu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}
		lo := n.lastApplied + 1
		hi := n.commitIndex
		n.mu.Unlock()

		entries, err := n.log.ReadRange(lo, hi)
		if err != nil {
			return
		}

		for _, e := range entries {
			result := n.sm.Apply(e.Cmd)

			n.mu.Lock()
			n.lastApplied = e.Index
			n.mu.Unlock()

			// Notify pending proposal if any
			n.pendingMu.Lock()
			if ch, ok := n.pending[e.Index]; ok {
				ch <- result
			}
			n.pendingMu.Unlock()
		}
	}
}

// RaftHTTPHandler returns the Raft RPC HTTP handler for this node.
func (n *Node) RaftHTTPHandler() *transporthttp.RaftHTTPServer {
	return transporthttp.NewRaftHTTPServer(n)
}
