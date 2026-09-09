package kvstore

import (
	"container/list"
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

	// elem is nil while the store is unbounded. Its Value is the key, which is
	// what lets an eviction name the entry it took off the tail.
	elem *list.Element
}

func (e *memoryEntry) expired(now time.Time) bool {
	return !e.expires.IsZero() && !now.Before(e.expires)
}

// namespaceEntries is why the bound is per namespace: the recency list belongs
// to one namespace, so a flood in one cannot evict another's entries and Clear
// stays a single map delete. order is nil while the store is unbounded.
type namespaceEntries struct {
	items map[string]*memoryEntry
	order *list.List // MRU at the front
}

// MemoryStore is an in-process, per-replica Store. Reads hide expired entries
// lazily; a sweep after every sweepEvery writes reclaims them. Unbounded by
// default: see WithMaxEntriesPerNamespace.
type MemoryStore struct {
	mu     sync.RWMutex
	data   map[string]*namespaceEntries
	writes int

	// max is immutable after construction, which is what lets the read paths
	// branch on it without holding the lock.
	max int

	now func() time.Time
}

// MemoryOption configures a MemoryStore. It is separate from Option, which
// describes the Postgres store's physical layout.
type MemoryOption func(*MemoryStore)

// WithMaxEntriesPerNamespace bounds each namespace independently, so a flood in
// one cannot evict another's entries. The ceiling on the whole store is
// therefore this count times the number of namespaces, which an application
// can compute because its namespaces are constants.
//
// A count of zero or less is unbounded, and unbounded is the default. Note what
// that means: the amortized sweep only reclaims what expired, so a namespace
// written with no expiry grows without limit until a bound is set.
func WithMaxEntriesPerNamespace(n int) MemoryOption {
	return func(s *MemoryStore) { s.max = n }
}

func NewMemory(opts ...MemoryOption) *MemoryStore {
	store := &MemoryStore{data: map[string]*namespaceEntries{}, now: time.Now}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

func (s *MemoryStore) bounded() bool { return s.max > 0 }

func (s *MemoryStore) newNamespace() *namespaceEntries {
	entries := &namespaceEntries{items: map[string]*memoryEntry{}}
	if s.bounded() {
		entries.order = list.New()
	}
	return entries
}

// lookupLocked is the only place the expiry check lives, so the two read paths
// cannot drift on what "visible" means. Read-only, so a read lock is enough.
func (s *MemoryStore) lookupLocked(namespace, key string) (*memoryEntry, bool) {
	entries := s.data[namespace]
	if entries == nil {
		return nil, false
	}
	entry, ok := entries.items[key]
	if !ok || entry.expired(s.now()) {
		return nil, false
	}
	return entry, true
}

func (s *MemoryStore) Get(ctx context.Context, namespace, key string) (json.RawMessage, bool, error) {
	entry, found, err := s.GetEntry(ctx, namespace, key)
	return entry.Value, found, err
}

func (s *MemoryStore) GetEntry(_ context.Context, namespace, key string) (Entry, bool, error) {
	// A bound means promoting on read, and promoting writes to the list, so the
	// bounded path needs the write lock. The unbounded path keeps the read lock
	// it always had: the LRU is the only reason to serialize readers.
	//
	// Either way the lock is released before returning, because Set replaces an
	// entry rather than mutating one, so the value slice stays valid after.
	if !s.bounded() {
		s.mu.RLock()
		entry, ok := s.lookupLocked(namespace, key)
		var out Entry
		if ok {
			out = Entry{Value: entry.value, Expires: entry.expires}
		}
		s.mu.RUnlock()
		return out, ok, nil
	}

	s.mu.Lock()
	entry, ok := s.lookupLocked(namespace, key)
	var out Entry
	if ok {
		s.data[namespace].order.MoveToFront(entry.elem)
		out = Entry{Value: entry.value, Expires: entry.expires}
	}
	s.mu.Unlock()
	return out, ok, nil
}

func (s *MemoryStore) Set(_ context.Context, namespace, key string, value json.RawMessage, expires time.Time) error {
	s.mu.Lock()
	if s.writes++; s.writes >= sweepEvery {
		s.sweepLocked(s.now())
	}
	entries := s.data[namespace]
	if entries == nil {
		entries = s.newNamespace()
		s.data[namespace] = entries
	}

	// Replaced rather than mutated in place, so a reader that already took the
	// old entry holds a value nothing writes to.
	entry := &memoryEntry{value: value, expires: expires}
	switch existing, ok := entries.items[key]; {
	case ok && existing.elem != nil:
		entry.elem = existing.elem
		entries.order.MoveToFront(entry.elem)
	case entries.order != nil:
		entry.elem = entries.order.PushFront(key)
	}
	entries.items[key] = entry

	if s.bounded() {
		for entries.order.Len() > s.max {
			evictLocked(entries)
		}
	}
	s.mu.Unlock()
	return nil
}

// evictLocked drops the least recently used entry, which the list tail names.
// The namespace always keeps at least the entry just written, because a bound
// is at least one.
func evictLocked(entries *namespaceEntries) {
	back := entries.order.Back()
	if back == nil {
		return
	}
	entries.order.Remove(back)
	delete(entries.items, back.Value.(string))
}

func (s *MemoryStore) Delete(_ context.Context, namespace string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	s.mu.Lock()
	if entries := s.data[namespace]; entries != nil {
		for _, key := range keys {
			entry, ok := entries.items[key]
			if !ok {
				continue
			}
			if entry.elem != nil {
				entries.order.Remove(entry.elem)
			}
			delete(entries.items, key)
		}
		if len(entries.items) == 0 {
			delete(s.data, namespace)
		}
	}
	s.mu.Unlock()
	return nil
}

// Has does not promote the entry: a probe is not a use.
func (s *MemoryStore) Has(_ context.Context, namespace, key string) (bool, error) {
	s.mu.RLock()
	_, ok := s.lookupLocked(namespace, key)
	s.mu.RUnlock()
	return ok, nil
}

func (s *MemoryStore) Clear(_ context.Context, namespace string) error {
	s.mu.Lock()
	delete(s.data, namespace)
	s.mu.Unlock()
	return nil
}

// sweepLocked rebuilds each namespace rather than deleting key by key, because
// a Go map never releases its buckets: a namespace that ballooned and then
// expired would hold the memory for good. The recency list is rebuilt in the
// same pass, so the survivors keep their order.
func (s *MemoryStore) sweepLocked(now time.Time) {
	s.writes = 0
	for namespace, entries := range s.data {
		kept := make(map[string]*memoryEntry, len(entries.items))
		var order *list.List

		if entries.order == nil {
			for key, entry := range entries.items {
				if !entry.expired(now) {
					kept[key] = entry
				}
			}
		} else {
			order = list.New()
			for elem := entries.order.Front(); elem != nil; elem = elem.Next() {
				key := elem.Value.(string)
				entry := entries.items[key]
				if entry.expired(now) {
					continue
				}
				entry.elem = order.PushBack(key)
				kept[key] = entry
			}
		}

		if len(kept) == 0 {
			delete(s.data, namespace)
		} else {
			s.data[namespace] = &namespaceEntries{items: kept, order: order}
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
		total += len(entries.items)
	}
	return total
}
