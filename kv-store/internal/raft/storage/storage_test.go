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
