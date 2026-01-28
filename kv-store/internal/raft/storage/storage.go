package storage

import (
	"fmt"
	"sync"

	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// LogEntry is a single entry in the Raft log.
type LogEntry struct {
	Index uint64        `json:"index"`
	Term  uint64        `json:"term"`
	Cmd   types.Command `json:"cmd"`
}

// SnapshotMeta holds metadata about a snapshot.
type SnapshotMeta struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

// --- Interfaces ---

// StableStore persists Raft durable state (term, vote).
type StableStore interface {
	GetCurrentTerm() (uint64, error)
	SetCurrentTerm(uint64) error
	GetVotedFor() (types.NodeID, bool, error)
	SetVotedFor(types.NodeID) error
}

// LogStore persists the Raft log.
type LogStore interface {
	LastIndex() (uint64, error)
	TermAt(index uint64) (uint64, error)
	Append(entries []LogEntry) error
	ReadRange(lo, hi uint64) ([]LogEntry, error)
	DeleteFrom(index uint64) error
	TruncatePrefix(upto uint64) error
}

// SnapshotStore persists snapshots.
type SnapshotStore interface {
	Save(meta SnapshotMeta, data []byte) error
	Load() (meta SnapshotMeta, data []byte, ok bool, err error)
}

// --- Memory implementations ---

// MemStableStore is an in-memory StableStore.
type MemStableStore struct {
	mu       sync.Mutex
	term     uint64
	votedFor types.NodeID
	hasVote  bool
}

func NewMemStableStore() *MemStableStore {
	return &MemStableStore{}
}

func (s *MemStableStore) GetCurrentTerm() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, nil
}

func (s *MemStableStore) SetCurrentTerm(term uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	return nil
}

func (s *MemStableStore) GetVotedFor() (types.NodeID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.votedFor, s.hasVote, nil
}

func (s *MemStableStore) SetVotedFor(id types.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = id
	s.hasVote = true
	return nil
}

// MemLogStore is an in-memory LogStore. Index 0 is a dummy sentinel.
type MemLogStore struct {
	mu      sync.Mutex
	entries []LogEntry // entries[0] is sentinel
}

func NewMemLogStore() *MemLogStore {
	return &MemLogStore{
		entries: []LogEntry{{}}, // dummy at index 0
	}
}

func (s *MemLogStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.entries) - 1), nil
}

func (s *MemLogStore) TermAt(index uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 || int(index) >= len(s.entries) {
		return 0, fmt.Errorf("index %d out of range [1, %d]", index, len(s.entries)-1)
	}
	return s.entries[index].Term, nil
}

func (s *MemLogStore) Append(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		s.entries = append(s.entries, e)
	}
	return nil
}

func (s *MemLogStore) ReadRange(lo, hi uint64) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo < 1 || hi >= uint64(len(s.entries)) || lo > hi {
		return nil, fmt.Errorf("invalid range [%d, %d], log length %d", lo, hi, len(s.entries)-1)
	}
	// Return a copy
	result := make([]LogEntry, hi-lo+1)
	copy(result, s.entries[lo:hi+1])
	return result, nil
}

func (s *MemLogStore) DeleteFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 1 || int(index) >= len(s.entries) {
		return fmt.Errorf("index %d out of range [1, %d]", index, len(s.entries)-1)
	}
	s.entries = s.entries[:index]
	return nil
}

func (s *MemLogStore) TruncatePrefix(upto uint64) error {
	// No-op until M5
	return nil
}

// MemSnapshotStore is an in-memory SnapshotStore.
type MemSnapshotStore struct {
	mu   sync.Mutex
	meta SnapshotMeta
	data []byte
	ok   bool
}

func NewMemSnapshotStore() *MemSnapshotStore {
	return &MemSnapshotStore{}
}

func (s *MemSnapshotStore) Save(meta SnapshotMeta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = meta
	s.data = make([]byte, len(data))
	copy(s.data, data)
	s.ok = true
	return nil
}

func (s *MemSnapshotStore) Load() (SnapshotMeta, []byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ok {
		return SnapshotMeta{}, nil, false, nil
	}
	data := make([]byte, len(s.data))
	copy(data, s.data)
	return s.meta, data, true, nil
}
