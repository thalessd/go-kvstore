package kvstore

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// sweepEvery caps how long an expired-but-unread entry can linger in memory,
// by amortizing a full sweep over that many writes.
const sweepEvery = 2048

type memoryEntry struct {
	value   json.RawMessage
	expires time.Time // zero means no expiry
}

func (e memoryEntry) expired(now time.Time) bool {
	return !e.expires.IsZero() && !now.Before(e.expires)
}

// MemoryStore is an in-process, per-replica Store. Reads hide expired entries
// lazily; a sweep after every sweepEvery writes reclaims them.
type MemoryStore struct {
	mu     sync.RWMutex
	data   map[string]map[string]memoryEntry
	writes int

	now func() time.Time
}

func NewMemory() *MemoryStore {
	return &MemoryStore{data: map[string]map[string]memoryEntry{}, now: time.Now}
}

func (s *MemoryStore) Get(ctx context.Context, namespace, key string) (json.RawMessage, bool, error) {
	entry, found, err := s.GetEntry(ctx, namespace, key)
	return entry.Value, found, err
}

func (s *MemoryStore) GetEntry(_ context.Context, namespace, key string) (Entry, bool, error) {
	// RUnlock before returning: the entry value is never mutated in place, so
	// the slice stays valid after the lock is gone.
	s.mu.RLock()
	entry, ok := s.data[namespace][key]
	s.mu.RUnlock()
	if !ok || entry.expired(s.now()) {
		return Entry{}, false, nil
	}
	return Entry{Value: entry.value, Expires: entry.expires}, true, nil
}

func (s *MemoryStore) Set(_ context.Context, namespace, key string, value json.RawMessage, expires time.Time) error {
	s.mu.Lock()
	if s.writes++; s.writes >= sweepEvery {
		s.sweepLocked(s.now())
	}
	entries := s.data[namespace]
	if entries == nil {
		entries = map[string]memoryEntry{}
		s.data[namespace] = entries
	}
	entries[key] = memoryEntry{value: value, expires: expires}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, namespace string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	s.mu.Lock()
	entries := s.data[namespace]
	for _, key := range keys {
		delete(entries, key)
	}
	if len(entries) == 0 {
		delete(s.data, namespace)
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Has(_ context.Context, namespace, key string) (bool, error) {
	s.mu.RLock()
	entry, ok := s.data[namespace][key]
	s.mu.RUnlock()
	return ok && !entry.expired(s.now()), nil
}

func (s *MemoryStore) Clear(_ context.Context, namespace string) error {
	s.mu.Lock()
	delete(s.data, namespace)
	s.mu.Unlock()
	return nil
}

// sweepLocked rebuilds the map rather than deleting key by key, so the sweep
// is one pass under one write lock.
func (s *MemoryStore) sweepLocked(now time.Time) {
	s.writes = 0
	for namespace, entries := range s.data {
		kept := make(map[string]memoryEntry, len(entries))
		for key, entry := range entries {
			if !entry.expired(now) {
				kept[key] = entry
			}
		}
		if len(kept) == 0 {
			delete(s.data, namespace)
		} else {
			s.data[namespace] = kept
		}
	}
}

// PurgeExpired reclaims every already-expired entry and reports how many.
// The amortized sweep makes it unnecessary for correctness; it exists so a
// MemoryStore satisfies Purger and the conformance suite covers the sweep on
// both stores. The write counter resets as a side effect.
func (s *MemoryStore) PurgeExpired(_ context.Context, now time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	before := s.countLocked()
	s.sweepLocked(now)
	return int64(before - s.countLocked()), nil
}

func (s *MemoryStore) countLocked() int {
	total := 0
	for _, entries := range s.data {
		total += len(entries)
	}
	return total
}
