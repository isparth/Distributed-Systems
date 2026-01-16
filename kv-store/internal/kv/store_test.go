package kv

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestPutGet(t *testing.T) {
	s := NewKVStore(NoPersistence)

	s.Put(Entry{"a", "1"})
	v, ok := s.Get("a")
	if !ok || v != "1" {
		t.Fatalf("got %q,%v", v, ok)
	}

	s.Put(Entry{"a", "2"})
	v, ok = s.Get("a")
	if !ok || v != "2" {
		t.Fatalf("overwrite got %q,%v", v, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s := NewKVStore(NoPersistence)
	v, ok := s.Get("missing")
	if ok || v != "" {
		t.Fatalf("got %q,%v", v, ok)
	}
}

func TestDelete(t *testing.T) {
	s := NewKVStore(NoPersistence)
	s.Put(Entry{"a", "b"})
	s.Delete("a")
	_, ok := s.Get("a")
	if ok {
		t.Fatal("expected missing after delete")
	}

	// should not panic
	s.Delete("missing")
}

func TestCAS(t *testing.T) {
	s := NewKVStore(NoPersistence)
	s.Put(Entry{"a", "1"})

	if !s.CAS("a", "2", "1") {
		t.Fatal("expected CAS success")
	}
	v, _ := s.Get("a")
	if v != "2" {
		t.Fatalf("got %q", v)
	}

	if s.CAS("a", "3", "1") {
		t.Fatal("expected CAS fail")
	}
}

func TestPutEmptyKey(t *testing.T) {
	s := NewKVStore(NoPersistence)
	if s.Put(Entry{"", "x"}) {
		t.Fatal("expected Put to fail for empty key")
	}
}

func TestGetEmptyKey(t *testing.T) {
	s := NewKVStore(NoPersistence)
	if v, ok := s.Get(""); ok || v != "" {
		t.Fatalf("expected empty key to return false, got %q,%v", v, ok)
	}
}

func TestMPutEmptyKey(t *testing.T) {
	s := NewKVStore(NoPersistence)
	if s.MPut([]Entry{{"a", "1"}, {"", "2"}}) {
		t.Fatal("expected MPut to fail for empty key")
	}
}

func TestMGetIgnoresEmptyAndMissing(t *testing.T) {
	s := NewKVStore(NoPersistence)
	s.Put(Entry{"a", "1"})
	s.Put(Entry{"b", "2"})

	got := s.MGet([]string{"", "a", "missing"})
	want := map[string]string{"a": "1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestMDeleteCountsAndSkipsInvalid(t *testing.T) {
	s := NewKVStore(NoPersistence)
	s.Put(Entry{"a", "1"})
	s.Put(Entry{"b", "2"})

	deleted := s.MDelete([]string{"", "a", "missing", "b"})
	if deleted != 2 {
		t.Fatalf("got deleted=%d, want 2", deleted)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected key a to be deleted")
	}
	if _, ok := s.Get("b"); ok {
		t.Fatal("expected key b to be deleted")
	}
}

func TestCASMissingAndMismatch(t *testing.T) {
	s := NewKVStore(NoPersistence)
	if s.CAS("missing", "1", "0") {
		t.Fatal("expected CAS to fail for missing key")
	}
	s.Put(Entry{"a", "1"})
	if s.CAS("a", "2", "wrong") {
		t.Fatal("expected CAS to fail for wrong expected value")
	}
}

func TestSnapshotOnlyWritesSnapshot(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() {
		_ = os.Chdir(oldWD)
	}()

	s := NewKVStore(SnapshotOnly)
	s.snapshotRules = []SnapshotRule{{EverySeconds: 1, MinChanges: 1}}
	s.lastSnapshot = time.Now().Add(-2 * time.Second)
	s.opsSinceSnapshot = 0

	if !s.Put(Entry{"a", "1"}) {
		t.Fatal("expected Put to succeed")
	}

	path := filepath.Join(tmp, "snapshot.gob")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected snapshot file, got error: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected snapshot file to be non-empty")
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer f.Close()

	var data map[string]string
	if err := gob.NewDecoder(f).Decode(&data); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if got := data["a"]; got != "1" {
		t.Fatalf("snapshot value got %q, want %q", got, "1")
	}
	if s.opsSinceSnapshot != 0 {
		t.Fatalf("opsSinceSnapshot got %d, want 0", s.opsSinceSnapshot)
	}
}

func TestStrongWALWritesLog(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() {
		_ = os.Chdir(oldWD)
	}()

	s := NewKVStore(StrongWAL)
	if !s.Put(Entry{"a", "1"}) {
		t.Fatal("expected Put to succeed")
	}

	info, err := os.Stat(filepath.Join(tmp, "wal.log"))
	if err != nil {
		t.Fatalf("expected wal.log, got error: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected wal.log to be non-empty")
	}
}
