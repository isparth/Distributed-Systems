package kv

import (
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

var (
	ErrWALFull     = errors.New("wal full")
	ErrWALStopped  = errors.New("wal stopped")
	ErrWALDisabled = errors.New("wal disabled")
	ErrWALInternal = errors.New("wal internal")
)

type Entry struct {
	Key   string
	Value string
}

type SnapshotRule struct {
	EverySeconds int // time window
	MinChanges   int // ops threshold
}

type DurabilityMode int

const (
	NoPersistence DurabilityMode = iota
	SnapshotOnly
	StrongWAL // disk -> memory (sync)
	FastWAL   // memory -> disk (async)
)

const (
	defaultSnapshotEverySeconds = 15
	defaultSnapshotMinChanges   = 2
	walFileName                 = "wal.log"
	snapshotFileName            = "snapshot.gob"
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

func defaultSnapshotRules() []SnapshotRule {
	return []SnapshotRule{{
		EverySeconds: defaultSnapshotEverySeconds,
		MinChanges:   defaultSnapshotMinChanges,
	}}
}

type KVStore struct {
	mu      sync.Mutex
	data    map[string]string
	mode    DurabilityMode
	stopped bool

	// snapshot fields
	snapshotRules    []SnapshotRule
	lastSnapshot     time.Time
	opsSinceSnapshot int

	// Write ahead log fields.
	walCh      chan walRecord
	walDone    chan struct{}
	walEncoder *gob.Encoder
	walFile    *os.File
}

// the constructor
func NewKVStore(mode DurabilityMode) *KVStore {
	store := &KVStore{data: make(map[string]string), mode: mode}

	if mode == SnapshotOnly {
		store.snapshotRules = defaultSnapshotRules()
		store.lastSnapshot = time.Now()
	}

	// Creates a Write Ahead
	if mode == StrongWAL || mode == FastWAL {
		if err := store.initWAL(); err != nil {
			return store
		}
	}

	return store
}

func (store *KVStore) initWAL() error {
	f, err := os.OpenFile(walFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	store.walFile = f
	store.walEncoder = gob.NewEncoder(f)
	store.walCh = nil
	store.walDone = nil

	if store.mode == FastWAL {
		store.walCh = make(chan walRecord, 1024)
		store.walDone = make(chan struct{})
		enc := store.walEncoder
		ch := store.walCh
		done := store.walDone
		go func() {
			defer close(done)
			for rec := range ch {
				_ = enc.Encode(rec)
			}
		}()
	}

	return nil
}

// --------Snapshot Functionality --------//
func (store *KVStore) maybeSnapshot(rules []SnapshotRule) {
	now := time.Now()

	for _, rule := range rules {
		if int(now.Sub(store.lastSnapshot).Seconds()) >= rule.EverySeconds &&
			store.opsSinceSnapshot >= rule.MinChanges {

			_ = store.writeSnapshotLocked(snapshotFileName)
			store.lastSnapshot = now
			store.opsSinceSnapshot = 0
			return
		}
	}
}

func (store *KVStore) writeSnapshotLocked(path string) error {
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)

	if err := enc.Encode(store.data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readSnapshot(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]string), nil
		}
		return nil, err
	}
	defer f.Close()

	var data map[string]string
	if err := gob.NewDecoder(f).Decode(&data); err != nil {
		return nil, err
	}
	if data == nil {
		data = make(map[string]string)
	}
	return data, nil
}

// --------- Write Ahead Log Functionality --------------//
func replayWAL(path string, data map[string]string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	for {
		var rec walRecord
		if err := dec.Decode(&rec); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		applyWALRecord(data, rec)
	}
}

func applyWALRecord(data map[string]string, rec walRecord) {
	switch rec.Op {
	case opPut:
		if rec.Entry.Key == "" {
			return
		}
		data[rec.Entry.Key] = rec.Entry.Value
	case opMPut:
		for _, kv := range rec.Entries {
			if kv.Key == "" {
				continue
			}
			data[kv.Key] = kv.Value
		}
	case opDel:
		if rec.Key == "" {
			return
		}
		delete(data, rec.Key)
	case opMDel:
		for _, key := range rec.Keys {
			if key == "" {
				continue
			}
			delete(data, key)
		}
	case opCAS:
		if rec.Key == "" {
			return
		}
		if cur, ok := data[rec.Key]; ok && cur == rec.Expected {
			data[rec.Key] = rec.Value
		}
	}
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

func (store *KVStore) appendToWALAsync(rec walRecord) (err error) {
	ch := store.walCh
	if ch == nil {
		return ErrWALDisabled
	}
	defer func() {
		if recover() != nil {
			err = ErrWALStopped
		}
	}()
	select {
	case ch <- rec:
		return nil
	default:
		return ErrWALFull
	}
}

// -------- Key Value Store Functions -----------//
func (store *KVStore) Stop() bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.stopped {
		return true
	}
	store.stopped = true

	return true
}

func (store *KVStore) Reset() bool {
	store.mu.Lock()
	if store.stopped {
		store.mu.Unlock()
		return true
	}
	store.stopped = true
	store.data = make(map[string]string)
	store.opsSinceSnapshot = 0
	store.lastSnapshot = time.Time{}

	walCh := store.walCh
	walDone := store.walDone
	walFile := store.walFile

	store.walCh = nil
	store.walDone = nil
	store.walEncoder = nil
	store.walFile = nil
	store.mu.Unlock()

	if walCh != nil {
		close(walCh)
		if walDone != nil {
			<-walDone
		}
	}
	if walFile != nil {
		_ = walFile.Close()
	}
	return true
}

// Restart Operation it
func (store *KVStore) Restart() bool {
	if !store.Reset() {
		return false
	}

	store.mu.Lock()
	mode := store.mode
	store.mu.Unlock()

	data := make(map[string]string)
	switch mode {
	case SnapshotOnly:
		snapshotData, err := readSnapshot(snapshotFileName)
		if err != nil {
			return false
		}
		data = snapshotData
	case StrongWAL, FastWAL:
		if err := replayWAL(walFileName, data); err != nil {
			return false
		}
	}

	store.mu.Lock()
	store.data = data
	store.opsSinceSnapshot = 0
	store.lastSnapshot = time.Now()
	if mode == SnapshotOnly && len(store.snapshotRules) == 0 {
		store.snapshotRules = defaultSnapshotRules()
	}
	store.mu.Unlock()

	if mode == StrongWAL || mode == FastWAL {
		if err := store.initWAL(); err != nil {
			return false
		}
	}

	store.mu.Lock()
	store.stopped = false
	store.mu.Unlock()
	return true
}

func (store *KVStore) Get(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.stopped {
		return "", false
	}

	v, ok := store.data[key]
	return v, ok
}

func (store *KVStore) MGet(keys []string) map[string]string {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.stopped {
		return map[string]string{}
	}

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

// Put Operation
func (store *KVStore) Put(kv Entry) bool {
	if kv.Key == "" {
		return false
	}

	switch store.mode {
	case SnapshotOnly:
		store.mu.Lock()
		defer store.mu.Unlock()

		if store.stopped {
			return false
		}

		store.data[kv.Key] = kv.Value
		store.opsSinceSnapshot++
		store.maybeSnapshot(store.snapshotRules)

		return true

	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}

		if err := store.appendToWALSync(walRecord{Op: opPut, Entry: kv}); err != nil {
			return false
		}
		store.data[kv.Key] = kv.Value
		return true

	case FastWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}
		if err := store.appendToWALAsync(walRecord{Op: opPut, Entry: kv}); err != nil {
			return false
		}
		store.data[kv.Key] = kv.Value

		return true

	default:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}
		store.data[kv.Key] = kv.Value

		return true
	}
}

// Multiple Put Operation
func (store *KVStore) MPut(kvs []Entry) bool {
	for _, kv := range kvs {
		if kv.Key == "" {
			return false
		}
	}

	switch store.mode {
	case SnapshotOnly:
		store.mu.Lock()
		if store.stopped {
			store.mu.Unlock()
			return false
		}
		for _, kv := range kvs {
			store.data[kv.Key] = kv.Value
		}
		store.opsSinceSnapshot += len(kvs)
		store.maybeSnapshot(store.snapshotRules)
		store.mu.Unlock()
		return true

	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}

		if err := store.appendToWALSync(walRecord{Op: opMPut, Entries: kvs}); err != nil {
			return false
		}
		for _, kv := range kvs {
			store.data[kv.Key] = kv.Value
		}
		return true

	case FastWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}

		if err := store.appendToWALAsync(walRecord{Op: opMPut, Entries: kvs}); err != nil {
			return false
		}

		for _, kv := range kvs {
			store.data[kv.Key] = kv.Value
		}

		return true

	default:
		store.mu.Lock()
		if store.stopped {
			store.mu.Unlock()
			return false
		}
		for _, kv := range kvs {
			store.data[kv.Key] = kv.Value
		}
		store.mu.Unlock()
		return true
	}
}

// Delete Operation
func (store *KVStore) Delete(key string) bool {
	if key == "" {
		return false
	}

	switch store.mode {
	case SnapshotOnly:
		store.mu.Lock()
		if store.stopped {
			store.mu.Unlock()
			return false
		}
		_, existed := store.data[key]
		delete(store.data, key)
		if existed {
			store.opsSinceSnapshot++
			store.maybeSnapshot(store.snapshotRules)
		}
		store.mu.Unlock()
		return true

	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}

		if err := store.appendToWALSync(walRecord{Op: opDel, Key: key}); err != nil {
			return false
		}
		delete(store.data, key)
		return true

	case FastWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {

			return false
		}
		if err := store.appendToWALAsync(walRecord{Op: opDel, Key: key}); err != nil {
			return false
		}

		delete(store.data, key)

		return true

	default:
		store.mu.Lock()
		if store.stopped {
			store.mu.Unlock()
			return false
		}
		delete(store.data, key)
		store.mu.Unlock()
		return true
	}
}

// Multiple detele Operation
func (store *KVStore) MDelete(keys []string) int {

	switch store.mode {
	case SnapshotOnly:
		store.mu.Lock()
		if store.stopped {
			store.mu.Unlock()
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
		if count > 0 {
			store.opsSinceSnapshot += count
			store.maybeSnapshot(store.snapshotRules)
		}
		store.mu.Unlock()
		return count

	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return 0
		}

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
		defer store.mu.Unlock()

		if store.stopped {
			return 0
		}

		// WAL first
		if err := store.appendToWALAsync(walRecord{Op: opMDel, Keys: append([]string(nil), keys...)}); err != nil {
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

	default:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
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
	}
}

// CAS Operation
func (store *KVStore) CAS(key, value, expected string) bool {
	if key == "" {
		return false
	}

	switch store.mode {
	case SnapshotOnly:
		store.mu.Lock()
		defer store.mu.Unlock()

		if store.stopped {
			return false
		}
		cur, ok := store.data[key]
		if !ok || cur != expected {
			return false
		}
		store.data[key] = value
		store.opsSinceSnapshot++
		store.maybeSnapshot(store.snapshotRules)
		store.mu.Unlock()
		return true

	case StrongWAL:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}

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
		defer store.mu.Unlock()

		if store.stopped {
			return false
		}
		cur, ok := store.data[key]
		if !ok || cur != expected {
			return false
		}

		if err := store.appendToWALAsync(walRecord{Op: opCAS, Key: key, Value: value, Expected: expected}); err != nil {
			return false
		}

		store.data[key] = value
		return true

	default:
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.stopped {
			return false
		}

		cur, ok := store.data[key]
		if !ok || cur != expected {
			return false
		}
		store.data[key] = value
		return true
	}
}
