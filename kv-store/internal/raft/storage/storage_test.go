package storage

import (
	"testing"

	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

func TestMemLogStore_AppendReadRangeTermAt(t *testing.T) {
	s := NewMemLogStore()

	idx, _ := s.LastIndex()
	if idx != 0 {
		t.Fatalf("expected last index 0, got %d", idx)
	}

	entries := []LogEntry{
		{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "a", Value: "1"}},
		{Index: 2, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "b", Value: "2"}},
		{Index: 3, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "c", Value: "3"}},
	}
	if err := s.Append(entries); err != nil {
		t.Fatal(err)
	}

	idx, _ = s.LastIndex()
	if idx != 3 {
		t.Fatalf("expected last index 3, got %d", idx)
	}

	term, err := s.TermAt(2)
	if err != nil || term != 1 {
		t.Fatalf("expected term 1 at index 2, got %d err=%v", term, err)
	}
	term, err = s.TermAt(3)
	if err != nil || term != 2 {
		t.Fatalf("expected term 2 at index 3, got %d err=%v", term, err)
	}

	// ReadRange
	got, err := s.ReadRange(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}
	if got[0].Cmd.Key != "a" || got[2].Cmd.Key != "c" {
		t.Fatalf("entries mismatch: %+v", got)
	}

	// ReadRange partial
	got, err = s.ReadRange(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Cmd.Key != "b" {
		t.Fatalf("expected single entry b, got %+v", got)
	}

	// Returned slice should be a copy
	got[0].Cmd.Key = "modified"
	orig, _ := s.ReadRange(2, 2)
	if orig[0].Cmd.Key != "b" {
		t.Fatal("ReadRange returned internal slice reference")
	}
}

func TestMemLogStore_DeleteFrom(t *testing.T) {
	s := NewMemLogStore()
	entries := []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}
	s.Append(entries)

	if err := s.DeleteFrom(2); err != nil {
		t.Fatal(err)
	}

	idx, _ := s.LastIndex()
	if idx != 1 {
		t.Fatalf("expected last index 1 after delete, got %d", idx)
	}

	_, err := s.TermAt(2)
	if err == nil {
		t.Fatal("expected error for deleted index")
	}

	// DeleteFrom out of range
	err = s.DeleteFrom(5)
	if err == nil {
		t.Fatal("expected error for out of range")
	}
}

// --- M3 Tests ---

func TestMemLogStore_DeleteFrom_TruncatesSuffix(t *testing.T) {
	s := NewMemLogStore()
	entries := []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
	}
	s.Append(entries)

	// Delete from index 3 onwards (keep 1, 2)
	if err := s.DeleteFrom(3); err != nil {
		t.Fatal(err)
	}

	idx, _ := s.LastIndex()
	if idx != 2 {
		t.Fatalf("expected last index 2 after truncation, got %d", idx)
	}

	// Verify entries 1 and 2 still exist
	term, err := s.TermAt(1)
	if err != nil || term != 1 {
		t.Fatalf("entry 1 should still exist with term 1, got term=%d err=%v", term, err)
	}
	term, err = s.TermAt(2)
	if err != nil || term != 1 {
		t.Fatalf("entry 2 should still exist with term 1, got term=%d err=%v", term, err)
	}

	// Verify entries 3-5 are gone
	for i := uint64(3); i <= 5; i++ {
		_, err := s.TermAt(i)
		if err == nil {
			t.Fatalf("entry %d should be deleted", i)
		}
	}

	// Can append new entries after truncation
	newEntries := []LogEntry{
		{Index: 3, Term: 3},
		{Index: 4, Term: 3},
	}
	if err := s.Append(newEntries); err != nil {
		t.Fatal(err)
	}
	idx, _ = s.LastIndex()
	if idx != 4 {
		t.Fatalf("expected last index 4 after re-append, got %d", idx)
	}
	term, _ = s.TermAt(3)
	if term != 3 {
		t.Fatalf("new entry 3 should have term 3, got %d", term)
	}
}

func TestMemLogStore_TermAt_ErrorsOnMissing(t *testing.T) {
	s := NewMemLogStore()

	// Empty log - index 1 should error
	_, err := s.TermAt(1)
	if err == nil {
		t.Fatal("expected error for missing index 1 on empty log")
	}

	// Index 0 should always error (sentinel)
	_, err = s.TermAt(0)
	if err == nil {
		t.Fatal("expected error for index 0")
	}

	// Add some entries
	entries := []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	s.Append(entries)

	// Valid indices work
	term, err := s.TermAt(1)
	if err != nil || term != 1 {
		t.Fatalf("expected term 1, got %d err=%v", term, err)
	}
	term, err = s.TermAt(2)
	if err != nil || term != 2 {
		t.Fatalf("expected term 2, got %d err=%v", term, err)
	}

	// Index beyond log should error
	_, err = s.TermAt(3)
	if err == nil {
		t.Fatal("expected error for index 3 beyond log")
	}

	_, err = s.TermAt(100)
	if err == nil {
		t.Fatal("expected error for index 100 beyond log")
	}
}

func TestMemStableStore_TermVote(t *testing.T) {
	s := NewMemStableStore()

	term, _ := s.GetCurrentTerm()
	if term != 0 {
		t.Fatalf("expected initial term 0, got %d", term)
	}

	s.SetCurrentTerm(5)
	term, _ = s.GetCurrentTerm()
	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}

	_, hasVote, _ := s.GetVotedFor()
	if hasVote {
		t.Fatal("expected no vote initially")
	}

	s.SetVotedFor("node1")
	id, hasVote, _ := s.GetVotedFor()
	if !hasVote || id != "node1" {
		t.Fatalf("expected vote for node1, got %s hasVote=%v", id, hasVote)
	}
}

// --- M5 Tests ---

func TestLogStore_TruncatePrefix(t *testing.T) {
	s := NewMemLogStore()

	// Add entries 1-5
	entries := []LogEntry{
		{Index: 1, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "a", Value: "1"}},
		{Index: 2, Term: 1, Cmd: types.Command{Op: types.OpPut, Key: "b", Value: "2"}},
		{Index: 3, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "c", Value: "3"}},
		{Index: 4, Term: 2, Cmd: types.Command{Op: types.OpPut, Key: "d", Value: "4"}},
		{Index: 5, Term: 3, Cmd: types.Command{Op: types.OpPut, Key: "e", Value: "5"}},
	}
	s.Append(entries)

	// Verify initial state
	lastIdx, _ := s.LastIndex()
	if lastIdx != 5 {
		t.Fatalf("expected last index 5, got %d", lastIdx)
	}

	// Truncate prefix up to index 3
	if err := s.TruncatePrefix(3); err != nil {
		t.Fatalf("TruncatePrefix failed: %v", err)
	}

	// Verify base index and term
	if s.BaseIndex() != 3 {
		t.Fatalf("expected base index 3, got %d", s.BaseIndex())
	}
	if s.BaseTerm() != 2 {
		t.Fatalf("expected base term 2, got %d", s.BaseTerm())
	}

	// Verify last index is still 5
	lastIdx, _ = s.LastIndex()
	if lastIdx != 5 {
		t.Fatalf("expected last index 5 after truncate, got %d", lastIdx)
	}

	// Verify can get term at baseIndex
	term, err := s.TermAt(3)
	if err != nil {
		t.Fatalf("TermAt(3) failed: %v", err)
	}
	if term != 2 {
		t.Fatalf("expected term 2 at index 3, got %d", term)
	}

	// Verify can read entries 4 and 5
	got, err := s.ReadRange(4, 5)
	if err != nil {
		t.Fatalf("ReadRange(4, 5) failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].Cmd.Key != "d" || got[1].Cmd.Key != "e" {
		t.Fatalf("wrong entries: %+v", got)
	}

	// Verify cannot read truncated entries
	_, err = s.ReadRange(1, 2)
	if err == nil {
		t.Fatal("expected error reading truncated entries")
	}

	// Verify can append new entries after truncation
	newEntries := []LogEntry{
		{Index: 6, Term: 3, Cmd: types.Command{Op: types.OpPut, Key: "f", Value: "6"}},
	}
	if err := s.Append(newEntries); err != nil {
		t.Fatalf("Append after truncate failed: %v", err)
	}

	lastIdx, _ = s.LastIndex()
	if lastIdx != 6 {
		t.Fatalf("expected last index 6 after append, got %d", lastIdx)
	}

	// Verify DeleteFrom works after truncation
	if err := s.DeleteFrom(5); err != nil {
		t.Fatalf("DeleteFrom(5) failed: %v", err)
	}
	lastIdx, _ = s.LastIndex()
	if lastIdx != 4 {
		t.Fatalf("expected last index 4 after delete, got %d", lastIdx)
	}
}

func TestLogStore_SetBase(t *testing.T) {
	s := NewMemLogStore()

	// SetBase to simulate snapshot restore
	s.SetBase(10, 5)

	if s.BaseIndex() != 10 {
		t.Fatalf("expected base index 10, got %d", s.BaseIndex())
	}
	if s.BaseTerm() != 5 {
		t.Fatalf("expected base term 5, got %d", s.BaseTerm())
	}

	// Last index should be baseIndex (no entries after base)
	lastIdx, _ := s.LastIndex()
	if lastIdx != 10 {
		t.Fatalf("expected last index 10, got %d", lastIdx)
	}

	// TermAt baseIndex should return baseTerm
	term, err := s.TermAt(10)
	if err != nil {
		t.Fatalf("TermAt(10) failed: %v", err)
	}
	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}

	// Append new entries
	entries := []LogEntry{
		{Index: 11, Term: 5, Cmd: types.Command{Op: types.OpPut, Key: "a", Value: "1"}},
		{Index: 12, Term: 6, Cmd: types.Command{Op: types.OpPut, Key: "b", Value: "2"}},
	}
	s.Append(entries)

	lastIdx, _ = s.LastIndex()
	if lastIdx != 12 {
		t.Fatalf("expected last index 12, got %d", lastIdx)
	}

	// Read the new entries
	got, err := s.ReadRange(11, 12)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
}

func TestSnapshotStore_SaveLoad(t *testing.T) {
	s := NewMemSnapshotStore()

	// Initially no snapshot
	_, _, ok, err := s.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if ok {
		t.Fatal("expected no snapshot initially")
	}

	// Save a snapshot
	meta := SnapshotMeta{LastIncludedIndex: 100, LastIncludedTerm: 5}
	data := []byte(`{"kv":{"foo":"bar"},"dedupe":{}}`)
	if err := s.Save(meta, data); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load the snapshot
	loadedMeta, loadedData, ok, err := s.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !ok {
		t.Fatal("expected snapshot to be present")
	}
	if loadedMeta.LastIncludedIndex != 100 || loadedMeta.LastIncludedTerm != 5 {
		t.Fatalf("wrong meta: %+v", loadedMeta)
	}
	if string(loadedData) != string(data) {
		t.Fatalf("wrong data: %s", loadedData)
	}
}
