package kv

import (
	"sync"
)

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (store *KVStore) Get(key string) (string, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, exists := store.data[key]

	return value, exists
}

func (store *KVStore) Put(key string, value string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = value
	return true
}

func (store *KVStore) Delete(key string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.data, key)

	return true
}

func (store *KVStore) CAS(key string, value string, expected string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	val, _ := store.data[key]

	if val == expected {
		store.data[key] = value
		return true
	}

	return false
}
