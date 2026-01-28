package kvsm

import (
	"encoding/json"
	"sync"

	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// DedupeRecord tracks the last applied sequence for a client.
type DedupeRecord struct {
	LastSeq   uint64           `json:"last_seq"`
	LastReply types.ApplyResult `json:"last_reply"`
}

// Snapshot is the serializable state of the state machine.
type Snapshot struct {
	KV     map[string]string        `json:"kv"`
	Dedupe map[string]DedupeRecord  `json:"dedupe"`
}

// KVStateMachine is a deterministic, thread-safe key-value state machine.
type KVStateMachine struct {
	mu     sync.Mutex
	kv     map[string]string
	dedupe map[string]DedupeRecord
}

// New creates a new KVStateMachine.
func New() *KVStateMachine {
	return &KVStateMachine{
		kv:     make(map[string]string),
		dedupe: make(map[string]DedupeRecord),
	}
}

// Apply applies a command to the state machine.
func (sm *KVStateMachine) Apply(cmd types.Command) types.ApplyResult {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Deduplication check
	if cmd.ClientID != "" && cmd.Seq != 0 {
		if rec, ok := sm.dedupe[cmd.ClientID]; ok && rec.LastSeq >= cmd.Seq {
			return rec.LastReply
		}
	}

	result := sm.applyUnlocked(cmd)

	// Store dedupe record
	if cmd.ClientID != "" && cmd.Seq != 0 {
		sm.dedupe[cmd.ClientID] = DedupeRecord{
			LastSeq:   cmd.Seq,
			LastReply: result,
		}
	}

	return result
}

func (sm *KVStateMachine) applyUnlocked(cmd types.Command) types.ApplyResult {
	switch cmd.Op {
	case types.OpPut:
		if cmd.Key == "" {
			return types.ApplyResult{Ok: false, ErrCode: "bad_request", ErrMsg: "key is required"}
		}
		sm.kv[cmd.Key] = cmd.Value
		return types.ApplyResult{Ok: true}

	case types.OpDelete:
		if cmd.Key == "" {
			return types.ApplyResult{Ok: false, ErrCode: "bad_request", ErrMsg: "key is required"}
		}
		deleted := 0
		if _, exists := sm.kv[cmd.Key]; exists {
			delete(sm.kv, cmd.Key)
			deleted = 1
		}
		return types.ApplyResult{Ok: true, Deleted: deleted}

	case types.OpCAS:
		if cmd.Key == "" {
			return types.ApplyResult{Ok: false, ErrCode: "bad_request", ErrMsg: "key is required"}
		}
		current := sm.kv[cmd.Key] // missing key returns ""
		if current != cmd.Expected {
			return types.ApplyResult{Ok: false, ErrCode: "cas_failed"}
		}
		sm.kv[cmd.Key] = cmd.Value
		return types.ApplyResult{Ok: true}

	case types.OpBatchPut:
		if len(cmd.Entries) == 0 {
			return types.ApplyResult{Ok: false, ErrCode: "bad_request", ErrMsg: "entries is required"}
		}
		for _, e := range cmd.Entries {
			sm.kv[e.Key] = e.Value
		}
		return types.ApplyResult{Ok: true}

	case types.OpBatchDelete:
		if len(cmd.Keys) == 0 {
			return types.ApplyResult{Ok: false, ErrCode: "bad_request", ErrMsg: "keys is required"}
		}
		deleted := 0
		for _, k := range cmd.Keys {
			if _, exists := sm.kv[k]; exists {
				delete(sm.kv, k)
				deleted++
			}
		}
		return types.ApplyResult{Ok: true, Deleted: deleted}

	default:
		return types.ApplyResult{Ok: false, ErrCode: "bad_request", ErrMsg: "unknown operation"}
	}
}

// Get returns the value for a key.
func (sm *KVStateMachine) Get(key string) (string, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	v, ok := sm.kv[key]
	return v, ok
}

// MGet returns values for multiple keys (cloned map).
func (sm *KVStateMachine) MGet(keys []string) map[string]string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	result := make(map[string]string, len(keys))
	for _, k := range keys {
		if v, ok := sm.kv[k]; ok {
			result[k] = v
		}
	}
	return result
}

// Snapshot serializes the state machine to JSON.
func (sm *KVStateMachine) Snapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snap := Snapshot{
		KV:     make(map[string]string, len(sm.kv)),
		Dedupe: make(map[string]DedupeRecord, len(sm.dedupe)),
	}
	for k, v := range sm.kv {
		snap.KV[k] = v
	}
	for k, v := range sm.dedupe {
		snap.Dedupe[k] = v
	}
	return json.Marshal(snap)
}

// Restore replaces the state machine state from a JSON snapshot.
func (sm *KVStateMachine) Restore(data []byte) error {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.kv = snap.KV
	sm.dedupe = snap.Dedupe
	if sm.kv == nil {
		sm.kv = make(map[string]string)
	}
	if sm.dedupe == nil {
		sm.dedupe = make(map[string]DedupeRecord)
	}
	return nil
}

// LastSeen returns the last sequence number seen for a client.
func (sm *KVStateMachine) LastSeen(clientID string) (uint64, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	rec, ok := sm.dedupe[clientID]
	if !ok {
		return 0, false
	}
	return rec.LastSeq, true
}
