package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/storage"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/transporthttp"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

const (
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
)

var ErrNotLeader = errors.New("not leader")

// TimingConfig holds configurable timing parameters for elections and heartbeats.
type TimingConfig struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// DefaultTimingConfig returns sensible defaults for production.
func DefaultTimingConfig() TimingConfig {
	return TimingConfig{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

// Config holds configuration for a Raft node.
type Config struct {
	ID     types.NodeID
	Peers  []types.NodeID // other nodes (not including self)
	Addr   string         // this node's advertised address
	Timing TimingConfig
	Rand   *rand.Rand // optional: for deterministic randomness in tests
}

// Status exposes internal state for debugging.
type Status struct {
	ID          types.NodeID     `json:"id"`
	Role        string           `json:"role"`
	Term        uint64           `json:"term"`
	CommitIndex uint64           `json:"commit_index"`
	LastApplied uint64           `json:"last_applied"`
	LastIndex   uint64           `json:"last_index"`
	LeaderHint  types.LeaderHint `json:"leader_hint"`
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
	votedFor    types.NodeID
	leaderHint  types.LeaderHint
	commitIndex uint64
	lastApplied uint64

	matchIndex map[types.NodeID]uint64
	nextIndex  map[types.NodeID]uint64

	// timers and goroutines
	applierDone      chan struct{}
	applierCh        chan struct{}
	electionTimer    *time.Timer
	heartbeatTicker  *time.Ticker
	cancel           context.CancelFunc
	ctx              context.Context
	electionResetCh  chan struct{}
	heartbeatStopCh  chan struct{}

	// pending proposals waiting for apply
	pendingMu sync.Mutex
	pending   map[uint64]chan types.ApplyResult

	// random source
	rand *rand.Rand
}

// NewNode creates a new Raft node.
func NewNode(cfg Config, stable storage.StableStore, log storage.LogStore, snap storage.SnapshotStore, tp transporthttp.Transport, sm *kvsm.KVStateMachine) (*Node, error) {
	term, err := stable.GetCurrentTerm()
	if err != nil {
		return nil, err
	}

	votedFor, _, err := stable.GetVotedFor()
	if err != nil {
		return nil, err
	}

	if cfg.Timing.ElectionTimeoutMin == 0 {
		cfg.Timing = DefaultTimingConfig()
	}

	r := cfg.Rand
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	n := &Node{
		cfg:             cfg,
		stable:          stable,
		log:             log,
		snap:            snap,
		tp:              tp,
		sm:              sm,
		role:            RoleFollower,
		currentTerm:     term,
		votedFor:        votedFor,
		matchIndex:      make(map[types.NodeID]uint64),
		nextIndex:       make(map[types.NodeID]uint64),
		applierCh:       make(chan struct{}, 1),
		pending:         make(map[uint64]chan types.ApplyResult),
		electionResetCh: make(chan struct{}, 1),
		rand:            r,
	}

	return n, nil
}

// Start starts the applier loop and election timer.
func (n *Node) Start(ctx context.Context) error {
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.applierDone = make(chan struct{})
	go n.applierLoop()
	go n.electionLoop()
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

func (n *Node) randomElectionTimeout() time.Duration {
	min := n.cfg.Timing.ElectionTimeoutMin
	max := n.cfg.Timing.ElectionTimeoutMax
	delta := max - min
	return min + time.Duration(n.rand.Int63n(int64(delta)))
}

func (n *Node) resetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}

func (n *Node) electionLoop() {
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.electionResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.randomElectionTimeout())
		case <-timer.C:
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()
			if role != RoleLeader {
				n.startElection()
			}
			timer.Reset(n.randomElectionTimeout())
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID
	term := n.currentTerm
	n.stable.SetCurrentTerm(term)
	n.stable.SetVotedFor(n.cfg.ID)

	lastIdx, _ := n.log.LastIndex()
	var lastTerm uint64
	if lastIdx > 0 {
		lastTerm, _ = n.log.TermAt(lastIdx)
	}

	peers := make([]types.NodeID, len(n.cfg.Peers))
	copy(peers, n.cfg.Peers)
	n.mu.Unlock()

	req := transporthttp.RequestVoteRequest{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	votes := 1 // vote for self
	majority := (len(peers)+1)/2 + 1

	type voteResult struct {
		resp transporthttp.RequestVoteResponse
		err  error
	}
	results := make(chan voteResult, len(peers))

	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.Timing.ElectionTimeoutMin)
	defer cancel()

	for _, p := range peers {
		go func(peer types.NodeID) {
			if n.tp == nil {
				results <- voteResult{err: fmt.Errorf("no transport")}
				return
			}
			resp, err := n.tp.RequestVote(ctx, peer, req)
			results <- voteResult{resp, err}
		}(p)
	}

	for range peers {
		select {
		case <-ctx.Done():
			return
		case vr := <-results:
			if vr.err != nil {
				continue
			}
			if vr.resp.Term > term {
				n.stepDown(vr.resp.Term)
				return
			}
			if vr.resp.VoteGranted {
				votes++
			}
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check we're still candidate for the same term
	if n.role != RoleCandidate || n.currentTerm != term {
		return
	}

	if votes >= majority {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderHint = types.LeaderHint{LeaderID: n.cfg.ID, LeaderAddr: n.cfg.Addr}

	lastIdx, _ := n.log.LastIndex()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}

	// Start heartbeat goroutine
	n.heartbeatStopCh = make(chan struct{})
	go n.heartbeatLoop()
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.cfg.Timing.HeartbeatInterval)
	defer ticker.Stop()

	// Send initial heartbeat immediately
	n.sendHeartbeats()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.heartbeatStopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.role == RoleLeader
			n.mu.Unlock()
			if !isLeader {
				return
			}
			n.sendHeartbeats()
		}
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	commitIndex := n.commitIndex
	lastIdx, _ := n.log.LastIndex()
	var prevLogTerm uint64
	if lastIdx > 0 {
		prevLogTerm, _ = n.log.TermAt(lastIdx)
	}
	peers := make([]types.NodeID, len(n.cfg.Peers))
	copy(peers, n.cfg.Peers)
	n.mu.Unlock()

	req := transporthttp.AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.cfg.ID,
		LeaderAddr:   n.cfg.Addr,
		PrevLogIndex: lastIdx,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIndex,
	}

	for _, p := range peers {
		go func(peer types.NodeID) {
			if n.tp == nil {
				return
			}
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.Timing.HeartbeatInterval)
			defer cancel()
			resp, err := n.tp.AppendEntries(ctx, peer, req)
			if err == nil && resp.Term > term {
				n.stepDown(resp.Term)
			}
		}(p)
	}
}

func (n *Node) stepDown(newTerm uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if newTerm > n.currentTerm {
		n.currentTerm = newTerm
		n.stable.SetCurrentTerm(newTerm)
		n.votedFor = ""
		// Note: don't call SetVotedFor with empty - votedFor is effectively cleared
	}
	if n.role == RoleLeader && n.heartbeatStopCh != nil {
		close(n.heartbeatStopCh)
		n.heartbeatStopCh = nil
	}
	n.role = RoleFollower
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
			if n.tp == nil {
				results <- peerResult{err: fmt.Errorf("no transport")}
				return
			}
			resp, err := n.tp.AppendEntries(ctx, peer, req)
			results <- peerResult{resp, err}
		}(p)
	}

	for range peers {
		pr := <-results
		if pr.err == nil && pr.resp.Success {
			successCount++
		}
		if pr.err == nil && pr.resp.Term > term {
			n.stepDown(pr.resp.Term)
			return types.ApplyResult{}, ErrNotLeader
		}
	}

	if successCount < majority {
		return types.ApplyResult{}, fmt.Errorf("failed to replicate to majority: %d/%d", successCount, majority)
	}

	// Advance commitIndex
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return types.ApplyResult{}, ErrNotLeader
	}
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
		if n.tp != nil {
			go n.tp.AppendEntries(ctx, p, commitNotify)
		}
	}

	// Wait for apply
	select {
	case res := <-resultCh:
		return res, nil
	case <-ctx.Done():
		return types.ApplyResult{}, ctx.Err()
	}
}

// HandleAppendEntries handles an incoming AppendEntries RPC.
func (n *Node) HandleAppendEntries(ctx context.Context, req transporthttp.AppendEntriesRequest) (transporthttp.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Step down if higher term
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.stable.SetCurrentTerm(req.Term)
		n.votedFor = ""
		if n.role == RoleLeader && n.heartbeatStopCh != nil {
			close(n.heartbeatStopCh)
			n.heartbeatStopCh = nil
		}
		n.role = RoleFollower
	}

	// Reject if our term is higher
	if req.Term < n.currentTerm {
		return transporthttp.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	// Valid AppendEntries from leader - reset election timer
	n.resetElectionTimer()

	// Update leader hint
	n.leaderHint = types.LeaderHint{LeaderID: req.LeaderID, LeaderAddr: req.LeaderAddr}

	// If we're candidate and receive AppendEntries from current term's leader, step down
	if n.role == RoleCandidate {
		n.role = RoleFollower
	}

	// Append entries (no conflict checking in M1/M2)
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

// HandleRequestVote handles an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(ctx context.Context, req transporthttp.RequestVoteRequest) (transporthttp.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Step down if higher term
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.stable.SetCurrentTerm(req.Term)
		n.votedFor = ""
		if n.role == RoleLeader && n.heartbeatStopCh != nil {
			close(n.heartbeatStopCh)
			n.heartbeatStopCh = nil
		}
		n.role = RoleFollower
	}

	// Reject if our term is higher
	if req.Term < n.currentTerm {
		return transporthttp.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}

	// Check if we can vote for this candidate
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID

	// Check log is at least as up-to-date
	lastIdx, _ := n.log.LastIndex()
	var lastTerm uint64
	if lastIdx > 0 {
		lastTerm, _ = n.log.TermAt(lastIdx)
	}

	logOK := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

	if canVote && logOK {
		n.votedFor = req.CandidateID
		n.stable.SetVotedFor(req.CandidateID)
		n.resetElectionTimer()
		return transporthttp.RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
	}

	return transporthttp.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
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
