package kvstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// recordingStore captures what the Cache layer hands down, standing in for a
// real Store without a database.
type recordingStore struct {
	setCalls []setCall
	raw      json.RawMessage
	found    bool
	err      error
}

type setCall struct {
	namespace, key string
	value          json.RawMessage
	expires        time.Time
}

func (s *recordingStore) Get(ctx context.Context, namespace, key string) (json.RawMessage, bool, error) {
	entry, found, err := s.GetEntry(ctx, namespace, key)
	return entry.Value, found, err
}

// GetEntry is the primitive here too, so the fake cannot answer a Get the way
// no real backend would.
func (s *recordingStore) GetEntry(_ context.Context, _, _ string) (Entry, bool, error) {
	return Entry{Value: s.raw}, s.found, s.err
}

func (s *recordingStore) Set(_ context.Context, namespace, key string, value json.RawMessage, expires time.Time) error {
	s.setCalls = append(s.setCalls, setCall{namespace, key, value, expires})
	return nil
}

func (s *recordingStore) Delete(_ context.Context, _ string, _ ...string) error { return nil }

func (s *recordingStore) Has(_ context.Context, _, _ string) (bool, error) { return false, nil }

func (s *recordingStore) Clear(_ context.Context, _ string) error { return nil }

func newTestCache(store Store, ttl time.Duration) *Cache[string] {
	c := NewCache[string](store, "test", ttl)
	c.now = func() time.Time { return time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC) }
	return c
}

type payload struct {
	Name string `json:"name"`
	N    int    `json:"n"`
}

func TestCacheRoundTrip(t *testing.T) {
	store := &recordingStore{raw: json.RawMessage(`{"name":"widget","n":7}`), found: true}
	cache := NewCache[payload](store, "ns", 0)

	got, found, err := cache.Get(t.Context(), "k1")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.Name != "widget" || got.N != 7 {
		t.Errorf("value = %+v", got)
	}

	if err := cache.Set(t.Context(), "k1", payload{Name: "x", N: 1}); err != nil {
		t.Fatalf("set: %v", err)
	}
	call := store.setCalls[0]
	if call.namespace != "ns" || call.key != "k1" {
		t.Errorf("namespace/key = %s/%s", call.namespace, call.key)
	}
	assertJSON(t, call.value, `{"name":"x","n":1}`)
}

func TestCacheTTL(t *testing.T) {
	fixed := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

	t.Run("default applies", func(t *testing.T) {
		store := &recordingStore{}
		cache := newTestCache(store, time.Minute)
		if err := cache.Set(t.Context(), "k", "v"); err != nil {
			t.Fatalf("set: %v", err)
		}
		if got, want := store.setCalls[0].expires, fixed.Add(time.Minute); !got.Equal(want) {
			t.Errorf("expires = %v, want %v", got, want)
		}
	})

	t.Run("explicit ttl wins", func(t *testing.T) {
		store := &recordingStore{}
		cache := newTestCache(store, time.Minute)
		if err := cache.Set(t.Context(), "k", "v", 2*time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		if got, want := store.setCalls[0].expires, fixed.Add(2*time.Minute); !got.Equal(want) {
			t.Errorf("expires = %v, want %v", got, want)
		}
	})

	t.Run("explicit zero means no expiry", func(t *testing.T) {
		store := &recordingStore{}
		cache := newTestCache(store, time.Minute)
		if err := cache.Set(t.Context(), "k", "v", 0); err != nil {
			t.Fatalf("set: %v", err)
		}
		if got := store.setCalls[0].expires; !got.IsZero() {
			t.Errorf("expires = %v, want zero", got)
		}
	})
}

func TestCacheGetSurfacesStoreAndDecodeFailures(t *testing.T) {
	cache := newTestCache(&recordingStore{err: context.Canceled}, 0)
	if _, _, err := cache.Get(t.Context(), "k"); err == nil {
		t.Error("store error was swallowed")
	}

	cache = newTestCache(&recordingStore{raw: json.RawMessage(`{not json`), found: true}, 0)
	if _, _, err := cache.Get(t.Context(), "k"); err == nil {
		t.Error("invalid payload did not fail the get")
	}
}
