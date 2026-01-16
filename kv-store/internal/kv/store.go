package kv

import (
	"encoding/gob"
	"os"
	"sync"
)

type DurabilityMode int

const (
	NoPersistence DurabilityMode = iota
	SnapshotOnly
	StrongWAL // disk -> memory (sync)
	FastWAL   // memory -> disk (async)
)

type walOp uint8

const (
	opPut walOp = iota + 1
	opMPut
	opDel
	opMDel
	opCAS
)

type walRecord struct {
	Op walOp

	// Put/MPut
	Entry   Entry
	Entries []Entry

	// Del/MDel/CAS
	Key      string
	Keys     []string
	Value    string
	Expected string
}

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
	mode DurabilityMode

	walCh      chan walRecord
	walEncoder *gob.Encoder
	walFile    *os.File
}

type Entry struct {
	Key   string
	Value string
}

func NewKVStore(mode DurabilityMode) *KVStore {
	store := &KVStore{data: make(map[string]string), mode: mode}

	// Creates a Write Ahead
	if mode == StrongWAL || mode == FastWAL {
		f, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return store
		}
		store.walFile = f
		store.walEncoder = gob.NewEncoder(f)
	}

	if mode == FastWAL {
		store.walCh = make(chan walRecord, 1024)
		go func() {
			for rec := range store.walCh {
				_ = store.walEncoder.Encode(rec)
			}
		}()
	}

	return store
}

func (store *KVStore) appendToWALSync(rec walRecord) error {
	if store.walEncoder == nil || store.walFile == nil {
		return nil
	}
	if err := store.walEncoder.Encode(rec); err != nil {
		return err
	}
	return store.walFile.Sync()
}

func (store *KVStore) appendToWALAsync(rec walRecord) bool {
	if store.walCh == nil {
		return true
	}
	select {
	case store.walCh <- rec:
		return true
	default:
		// queue full: choose backpressure vs drop
		return false
	}
}

func (store *KVStore) Get(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	v, ok := store.data[key]
	return v, ok
}

func (store *KVStore) MGet(keys []string) map[string]string {
	store.mu.Lock()
	defer store.mu.Unlock()

	out := make(map[string]string, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if v, ok := store.data[key]; ok {
			out[key] = v
		}
	}
	return out
}

func (store *KVStore) Put(kv Entry) bool {
	if kv.Key == "" {
		return false
	}

	switch store.mode {
	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()

		if err := store.appendToWALSync(walRecord{Op: opDel, Entry: kv}); err != nil {
			return false
		}
		store.data[kv.Key] = kv.Value
		return true

	case FastWAL:
		store.mu.Lock()
		store.data[kv.Key] = kv.Value
		store.mu.Unlock()

		return store.appendToWALAsync(walRecord{Op: opDel, Entry: kv})

	default:
		store.mu.Lock()
		store.data[kv.Key] = kv.Value
		store.mu.Unlock()
		return true
	}
}

func (store *KVStore) MPut(kvs []Entry) bool {
	for _, kv := range kvs {
		if kv.Key == "" {
			return false
		}
	}

	switch store.mode {
	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()

		if err := store.appendToWALSync(walRecord{Op: opDel, Entries: kvs}); err != nil {
			return false
		}
		for _, kv := range kvs {
			store.data[kv.Key] = kv.Value
		}
		return true

	case FastWAL:
		store.mu.Lock()
		for _, kv := range kvs {
			store.data[kv.Key] = kv.Value
		}
		store.mu.Unlock()

		return store.appendToWALAsync(walRecord{Op: opDel, Entries: kvs})

	default:
		store.mu.Lock()
		for _, kv := range kvs {
			store.data[kv.Key] = kv.Value
		}
		store.mu.Unlock()
		return true
	}
}

func (store *KVStore) Delete(key string) bool {
	if key == "" {
		return false
	}

	switch store.mode {
	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()

		if err := store.appendToWALSync(walRecord{Op: opDel, Key: key}); err != nil {
			return false
		}
		delete(store.data, key)
		return true

	case FastWAL:
		store.mu.Lock()
		delete(store.data, key)
		store.mu.Unlock()

		return store.appendToWALAsync(walRecord{Op: opDel, Key: key})

	default:
		store.mu.Lock()
		delete(store.data, key)
		store.mu.Unlock()
		return true
	}
}

func (store *KVStore) MDelete(keys []string) int {
	// Redis-style: ignore empty keys / missing keys; count actual deletions
	switch store.mode {
	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()

		// WAL first
		if err := store.appendToWALSync(walRecord{Op: opMDel, Keys: append([]string(nil), keys...)}); err != nil {
			return 0
		}

		count := 0
		for _, key := range keys {
			if key == "" {
				continue
			}
			if _, ok := store.data[key]; ok {
				delete(store.data, key)
				count++
			}
		}
		return count

	case FastWAL:
		store.mu.Lock()
		count := 0
		for _, key := range keys {
			if key == "" {
				continue
			}
			if _, ok := store.data[key]; ok {
				delete(store.data, key)
				count++
			}
		}
		store.mu.Unlock()

		ok := store.appendToWALAsync(walRecord{Op: opMDel, Keys: append([]string(nil), keys...)})
		if !ok {
			// memory already changed; you can’t “undo” without extra machinery
			// returning count still reflects what happened in memory
		}
		return count

	default:
		store.mu.Lock()
		defer store.mu.Unlock()

		count := 0
		for _, key := range keys {
			if key == "" {
				continue
			}
			if _, ok := store.data[key]; ok {
				delete(store.data, key)
				count++
			}
		}
		return count
	}
}

func (store *KVStore) CAS(key, value, expected string) bool {
	if key == "" {
		return false
	}

	switch store.mode {
	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()

		// check first, then WAL+apply (so you only log successful CAS)
		cur, ok := store.data[key]
		if !ok || cur != expected {
			return false
		}

		if err := store.appendToWALSync(walRecord{Op: opCAS, Key: key, Value: value, Expected: expected}); err != nil {
			return false
		}

		store.data[key] = value
		return true

	case FastWAL:
		store.mu.Lock()
		cur, ok := store.data[key]
		if !ok || cur != expected {
			store.mu.Unlock()
			return false
		}
		store.data[key] = value
		store.mu.Unlock()

		return store.appendToWALAsync(walRecord{Op: opCAS, Key: key, Value: value, Expected: expected})

	default:
		store.mu.Lock()
		defer store.mu.Unlock()

		cur, ok := store.data[key]
		if !ok || cur != expected {
			return false
		}
		store.data[key] = value
		return true
	}
}
