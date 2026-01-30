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
	BaseIndex() uint64 // M5: returns the index of the last truncated entry (0 if none)
	BaseTerm() uint64  // M5: returns the term at base index (0 if none)
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
// After TruncatePrefix, baseIndex is the logical index of the last truncated entry
// and baseTerm is the term at that index.
type MemLogStore struct {
	mu        sync.Mutex
	entries   []LogEntry // entries[0] is sentinel or first entry after truncation
	baseIndex uint64     // M5: logical index of last truncated entry (0 initially)
	baseTerm  uint64     // M5: term at baseIndex (0 initially)
}

func NewMemLogStore() *MemLogStore {
	return &MemLogStore{
		entries: []LogEntry{{}}, // dummy at index 0
	}
}

func (s *MemLogStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// M5: logical index = baseIndex + physical length - 1
	// If baseIndex=0, entries[0] is sentinel, so lastIndex = len-1
	// If baseIndex>0, entries[0] is first entry after truncation (at baseIndex+1),
	// so lastIndex = baseIndex + len(entries)
	if s.baseIndex == 0 {
		return uint64(len(s.entries) - 1), nil
	}
	return s.baseIndex + uint64(len(s.entries)), nil
}

func (s *MemLogStore) TermAt(index uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// M5: Handle baseIndex for truncated logs
	if s.baseIndex > 0 {
		// After truncation, baseIndex is the last truncated index
		if index < s.baseIndex {
			return 0, fmt.Errorf("index %d is before baseIndex %d (truncated)", index, s.baseIndex)
		}
		if index == s.baseIndex {
			return s.baseTerm, nil
		}
		// Physical index: entries[0] is at logical index baseIndex+1
		physIdx := index - s.baseIndex - 1
		if physIdx >= uint64(len(s.entries)) {
			return 0, fmt.Errorf("index %d out of range [%d, %d]", index, s.baseIndex, s.baseIndex+uint64(len(s.entries)))
		}
		return s.entries[physIdx].Term, nil
	}

	// No truncation case (baseIndex == 0)
	if index == 0 || int(index) >= len(s.entries) {
		return 0, fmt.Errorf("index %d out of range [1, %d]", index, len(s.entries)-1)
	}
	return s.entries[index].Term, nil
}

func (s *MemLogStore) Append(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *MemLogStore) ReadRange(lo, hi uint64) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// M5: Handle baseIndex for truncated logs
	if s.baseIndex > 0 {
		// Minimum accessible index is baseIndex+1
		if lo <= s.baseIndex || hi <= s.baseIndex {
			return nil, fmt.Errorf("invalid range [%d, %d]: indices must be > baseIndex %d", lo, hi, s.baseIndex)
		}
		if lo > hi {
			return nil, fmt.Errorf("invalid range [%d, %d]: lo > hi", lo, hi)
		}
		// Physical indices
		physLo := lo - s.baseIndex - 1
		physHi := hi - s.baseIndex - 1
		if physHi >= uint64(len(s.entries)) {
			return nil, fmt.Errorf("invalid range [%d, %d]: hi beyond log end %d", lo, hi, s.baseIndex+uint64(len(s.entries)))
		}
		result := make([]LogEntry, physHi-physLo+1)
		copy(result, s.entries[physLo:physHi+1])
		return result, nil
	}

	// No truncation case
	if lo < 1 || hi >= uint64(len(s.entries)) || lo > hi {
		return nil, fmt.Errorf("invalid range [%d, %d], log length %d", lo, hi, len(s.entries)-1)
	}
	result := make([]LogEntry, hi-lo+1)
	copy(result, s.entries[lo:hi+1])
	return result, nil
}

func (s *MemLogStore) DeleteFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// M5: Handle baseIndex for truncated logs
	if s.baseIndex > 0 {
		if index <= s.baseIndex {
			return fmt.Errorf("index %d out of range: must be > baseIndex %d", index, s.baseIndex)
		}
		// Physical index
		physIdx := index - s.baseIndex - 1
		if physIdx >= uint64(len(s.entries)) {
			return fmt.Errorf("index %d out of range [%d, %d]", index, s.baseIndex+1, s.baseIndex+uint64(len(s.entries)))
		}
		s.entries = s.entries[:physIdx]
		return nil
	}

	// No truncation case
	if index < 1 || int(index) >= len(s.entries) {
		return fmt.Errorf("index %d out of range [1, %d]", index, len(s.entries)-1)
	}
	s.entries = s.entries[:index]
	return nil
}

func (s *MemLogStore) TruncatePrefix(upto uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if upto == 0 {
		return nil // nothing to truncate
	}

	// Get the term at the truncation point before modifying
	var termAtUpto uint64
	if s.baseIndex > 0 {
		// Already have a base
		if upto < s.baseIndex {
			return nil // already truncated past this point
		}
		if upto == s.baseIndex {
			return nil // already at this base
		}
		// upto > baseIndex
		physIdx := upto - s.baseIndex - 1
		if physIdx >= uint64(len(s.entries)) {
			return fmt.Errorf("cannot truncate to index %d: beyond log end %d", upto, s.baseIndex+uint64(len(s.entries)))
		}
		termAtUpto = s.entries[physIdx].Term
		// Keep entries from upto+1 onwards
		s.entries = s.entries[physIdx+1:]
	} else {
		// No prior truncation (baseIndex == 0)
		if upto >= uint64(len(s.entries)) {
			return fmt.Errorf("cannot truncate to index %d: beyond log end %d", upto, len(s.entries)-1)
		}
		termAtUpto = s.entries[upto].Term
		// Keep entries from upto+1 onwards
		s.entries = s.entries[upto+1:]
	}

	s.baseIndex = upto
	s.baseTerm = termAtUpto
	return nil
}

// BaseIndex returns the index of the last truncated entry (0 if no truncation).
func (s *MemLogStore) BaseIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.baseIndex
}

// BaseTerm returns the term at the base index (0 if no truncation).
func (s *MemLogStore) BaseTerm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.baseTerm
}

// SetBase sets the base index and term directly (used when restoring from snapshot).
func (s *MemLogStore) SetBase(index, term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.baseIndex = index
	s.baseTerm = term
	s.entries = nil // Clear any existing entries
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
