package kv

import (
	"testing"
)

func TestPutGet(t *testing.T) {
	s := NewKVStore()

	s.Put("a", "1")
	v, ok := s.Get("a")
	if !ok || v != "1" {
		t.Fatalf("got %q,%v", v, ok)
	}

	s.Put("a", "2")
	v, ok = s.Get("a")
	if !ok || v != "2" {
		t.Fatalf("overwrite got %q,%v", v, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s := NewKVStore()
	v, ok := s.Get("missing")
	if ok || v != "" {
		t.Fatalf("got %q,%v", v, ok)
	}
}

func TestDelete(t *testing.T) {
	s := NewKVStore()
	s.Put("a", "1")
	s.Delete("a")
	_, ok := s.Get("a")
	if ok {
		t.Fatal("expected missing after delete")
	}

	// should not panic
	s.Delete("missing")
}

func TestCAS(t *testing.T) {
	s := NewKVStore()
	s.Put("a", "1")

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
