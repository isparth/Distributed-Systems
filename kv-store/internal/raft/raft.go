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

func (n *Node) Status() types.NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	lastIdx, _ := n.log.LastIndex()
	return types.NodeStatus{
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

// replicateToPeer sends AppendEntries to a peer and handles backtracking.
// Returns (success, shouldRetry). shouldRetry is true if we should immediately retry with updated nextIndex.
func (n *Node) replicateToPeer(ctx context.Context, peer types.NodeID) (success bool, matchIdx uint64) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return false, 0
	}
	term := n.currentTerm
	nextIdx := n.nextIndex[peer]
	commitIndex := n.commitIndex

	// Get prevLog info
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 {
		var err error
		prevLogTerm, err = n.log.TermAt(prevLogIndex)
		if err != nil {
			// prevLogIndex is beyond our log - shouldn't happen normally
			n.mu.Unlock()
			return false, 0
		}
	}

	// Get entries to send
	lastIdx, _ := n.log.LastIndex()
	var entries []storage.LogEntry
	if nextIdx <= lastIdx {
		var err error
		entries, err = n.log.ReadRange(nextIdx, lastIdx)
		if err != nil {
			n.mu.Unlock()
			return false, 0
		}
	}
	n.mu.Unlock()

	req := transporthttp.AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.cfg.ID,
		LeaderAddr:   n.cfg.Addr,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	if n.tp == nil {
		return false, 0
	}

	resp, err := n.tp.AppendEntries(ctx, peer, req)
	if err != nil {
		return false, 0
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we're still leader for the same term
	if n.role != RoleLeader || n.currentTerm != term {
		return false, 0
	}

	if resp.Term > n.currentTerm {
		// Step down - release lock first since stepDown also locks
		n.mu.Unlock()
		n.stepDown(resp.Term)
		n.mu.Lock()
		return false, 0
	}

	if resp.Success {
		// Update matchIndex and nextIndex
		newMatchIdx := prevLogIndex + uint64(len(entries))
		if newMatchIdx > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatchIdx
		}
		n.nextIndex[peer] = newMatchIdx + 1
		return true, newMatchIdx
	}

	// AppendEntries failed due to log inconsistency - backtrack nextIndex
	if resp.ConflictTerm == 0 {
		// Follower's log is too short
		n.nextIndex[peer] = resp.ConflictIndex
	} else {
		// Find if we have any entries with the conflict term
		found := false
		for i := resp.ConflictIndex; i <= lastIdx; i++ {
			t, err := n.log.TermAt(i)
			if err != nil {
				break
			}
			if t == resp.ConflictTerm {
				// We have entries with this term - set nextIndex to after our last entry of this term
				for j := i; j <= lastIdx; j++ {
					t2, _ := n.log.TermAt(j)
					if t2 != resp.ConflictTerm {
						n.nextIndex[peer] = j
						found = true
						break
					}
				}
				if !found {
					n.nextIndex[peer] = lastIdx + 1
					found = true
				}
				break
			}
		}
		if !found {
			// We don't have the conflict term - use the follower's first index of conflict term
			n.nextIndex[peer] = resp.ConflictIndex
		}
	}

	// Clamp nextIndex to at least 1
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}

	return false, 0
}

// advanceCommitIndex checks if we can advance commitIndex based on matchIndex.
// M3: Only commits entries from current term (Raft safety).
func (n *Node) advanceCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != RoleLeader {
		return
	}

	lastIdx, _ := n.log.LastIndex()

	// Try to advance commit index
	for idx := n.commitIndex + 1; idx <= lastIdx; idx++ {
		// M3 safety: Only commit entries from current term
		term, err := n.log.TermAt(idx)
		if err != nil || term != n.currentTerm {
			continue
		}

		// Count replicas (including self)
		replicaCount := 1
		for _, peer := range n.cfg.Peers {
			if n.matchIndex[peer] >= idx {
				replicaCount++
			}
		}

		majority := (len(n.cfg.Peers)+1)/2 + 1
		if replicaCount >= majority {
			n.commitIndex = idx
		}
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

	// Update matchIndex for self
	n.matchIndex[n.cfg.ID] = newIdx

	peers := make([]types.NodeID, len(n.cfg.Peers))
	copy(peers, n.cfg.Peers)
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

	// Replicate to all peers with retries for backtracking
	type peerResult struct {
		peer     types.NodeID
		success  bool
		matchIdx uint64
	}
	results := make(chan peerResult, len(peers))

	for _, p := range peers {
		go func(peer types.NodeID) {
			// Retry loop for log repair
			for attempt := 0; attempt < 10; attempt++ {
				select {
				case <-ctx.Done():
					results <- peerResult{peer: peer, success: false}
					return
				default:
				}

				success, matchIdx := n.replicateToPeer(ctx, peer)
				if success {
					results <- peerResult{peer: peer, success: true, matchIdx: matchIdx}
					return
				}

				// Check if we're still leader
				n.mu.Lock()
				stillLeader := n.role == RoleLeader && n.currentTerm == term
				n.mu.Unlock()
				if !stillLeader {
					results <- peerResult{peer: peer, success: false}
					return
				}

				// Small delay before retry
				select {
				case <-ctx.Done():
					results <- peerResult{peer: peer, success: false}
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
			results <- peerResult{peer: peer, success: false}
		}(p)
	}

	// Collect results
	successCount := 1 // count self
	majority := (len(peers)+1)/2 + 1

	for range peers {
		select {
		case pr := <-results:
			if pr.success {
				successCount++
			}
		case <-ctx.Done():
			return types.ApplyResult{}, ctx.Err()
		}
	}

	// Check if still leader
	n.mu.Lock()
	if n.role != RoleLeader || n.currentTerm != term {
		n.mu.Unlock()
		return types.ApplyResult{}, ErrNotLeader
	}
	n.mu.Unlock()

	if successCount < majority {
		return types.ApplyResult{}, fmt.Errorf("failed to replicate to majority: %d/%d", successCount, majority)
	}

	// Advance commitIndex using proper commit rule
	n.advanceCommitIndex()

	// Signal applier
	n.signalApplier()

	// Notify followers of new commitIndex
	n.mu.Lock()
	commitIndex := n.commitIndex
	n.mu.Unlock()

	for _, p := range peers {
		if n.tp != nil {
			go func(peer types.NodeID) {
				n.mu.Lock()
				nextIdx := n.nextIndex[peer]
				prevIdx := nextIdx - 1
				var prevTerm uint64
				if prevIdx > 0 {
					prevTerm, _ = n.log.TermAt(prevIdx)
				}
				n.mu.Unlock()

				commitNotify := transporthttp.AppendEntriesRequest{
					Term:         term,
					LeaderID:     n.cfg.ID,
					LeaderAddr:   n.cfg.Addr,
					PrevLogIndex: prevIdx,
					PrevLogTerm:  prevTerm,
					LeaderCommit: commitIndex,
				}
				n.tp.AppendEntries(ctx, peer, commitNotify)
			}(p)
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

	// M3: Log consistency check
	// Check that prevLogIndex entry exists and has matching term
	if req.PrevLogIndex > 0 {
		lastIdx, _ := n.log.LastIndex()
		if req.PrevLogIndex > lastIdx {
			// We don't have the entry at prevLogIndex
			return transporthttp.AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: lastIdx + 1,
				ConflictTerm:  0,
			}, nil
		}

		prevTerm, err := n.log.TermAt(req.PrevLogIndex)
		if err != nil {
			// Entry doesn't exist (shouldn't happen if lastIdx check passed)
			return transporthttp.AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: req.PrevLogIndex,
				ConflictTerm:  0,
			}, nil
		}

		if prevTerm != req.PrevLogTerm {
			// Term mismatch - find the first index of the conflicting term
			conflictTerm := prevTerm
			conflictIndex := req.PrevLogIndex
			// Walk backwards to find first entry with this term
			for conflictIndex > 1 {
				t, err := n.log.TermAt(conflictIndex - 1)
				if err != nil || t != conflictTerm {
					break
				}
				conflictIndex--
			}
			return transporthttp.AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: conflictIndex,
				ConflictTerm:  conflictTerm,
			}, nil
		}
	}

	// Append entries - check for conflicts and truncate if needed
	if len(req.Entries) > 0 {
		lastIdx, _ := n.log.LastIndex()

		for i, entry := range req.Entries {
			if entry.Index <= lastIdx {
				// Entry already exists - check if terms match
				existingTerm, err := n.log.TermAt(entry.Index)
				if err == nil && existingTerm != entry.Term {
					// Conflict! Delete this entry and everything after
					if err := n.log.DeleteFrom(entry.Index); err != nil {
						return transporthttp.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
					}
					// Append remaining entries from this point
					if err := n.log.Append(req.Entries[i:]); err != nil {
						return transporthttp.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
					}
					break
				}
				// Terms match - skip this entry, it's already correct
			} else {
				// Entry doesn't exist - append this and all remaining entries
				if err := n.log.Append(req.Entries[i:]); err != nil {
					return transporthttp.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
				}
				break
			}
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
