package kvstore

import (
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"
)

// The store clock is injectable, so expiry is observed by moving time forward
// rather than waiting for it.
func TestMemoryExpiryFollowsTheStoreClock(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	store := NewMemory()
	store.now = func() time.Time { return now }

	if err := store.Set(t.Context(), "ns", "k", json.RawMessage(`1`), now.Add(time.Minute)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, found, err := store.Get(t.Context(), "ns", "k"); err != nil || !found {
		t.Fatalf("before expiry: found=%v err=%v", found, err)
	}

	now = now.Add(2 * time.Minute)
	if _, found, err := store.Get(t.Context(), "ns", "k"); err != nil || found {
		t.Errorf("after expiry: found=%v err=%v", found, err)
	}
	if ok, err := store.Has(t.Context(), "ns", "k"); err != nil || ok {
		t.Errorf("Has after expiry: ok=%v err=%v", ok, err)
	}
}

func TestMemorySweepReclaimsExpiredEntries(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	store := NewMemory()
	store.now = func() time.Time { return now }

	if err := store.Set(t.Context(), "ns", "stale", json.RawMessage(`1`), now.Add(-time.Second)); err != nil {
		t.Fatalf("set expired: %v", err)
	}

	// One write short of the threshold: the stale entry must still be there.
	store.writes = sweepEvery - 2
	if err := store.Set(t.Context(), "ns", "fresh", json.RawMessage(`2`), time.Time{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, ok := store.data["ns"]["stale"]; !ok {
		t.Fatal("sweep ran before the threshold")
	}

	// Cross the threshold: the sweep rebuilds the map without the stale entry
	// and drops the emptied namespace map on Delete.
	if err := store.Set(t.Context(), "ns", "fresh", json.RawMessage(`3`), time.Time{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, ok := store.data["ns"]["stale"]; ok {
		t.Error("expired entry survived the sweep")
	}
	if _, found, err := store.Get(t.Context(), "ns", "fresh"); err != nil || !found {
		t.Errorf("fresh entry lost to the sweep: found=%v err=%v", found, err)
	}

	if err := store.Delete(t.Context(), "ns", "fresh"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := store.data["ns"]; ok {
		t.Error("empty namespace map was kept alive")
	}
}

// Mirrors gokv's concurrent interactions test: the race detector is the
// assertion, the final readback proves every goroutine's write landed.
func TestMemoryConcurrentInteractions(t *testing.T) {
	store := NewMemory()
	ctx := t.Context()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func() {
			defer wg.Done()
			key := strconv.Itoa(i)
			_, _, _ = store.Get(ctx, "race", key)
			_ = store.Set(ctx, "race", key, json.RawMessage(`{}`), time.Time{})
			_, _, _ = store.Get(ctx, "race", key)
			_ = store.Delete(ctx, "race", key)
			_ = store.Set(ctx, "race", key, json.RawMessage(`{}`), time.Time{})
		}()
	}
	wg.Wait()

	for i := range goroutines {
		if _, found, err := store.Get(ctx, "race", strconv.Itoa(i)); err != nil || !found {
			t.Fatalf("key %d missing after concurrent run: found=%v err=%v", i, found, err)
		}
	}
}

// The sweep is amortized, so an explicit purge is the only way a caller can
// force reclamation and learn how much it freed.
func TestMemoryPurgeExpiredReportsWhatItReclaimed(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	store := NewMemory()
	store.now = func() time.Time { return now }

	if err := store.Set(t.Context(), "ns", "stale", json.RawMessage(`1`), now.Add(-time.Minute)); err != nil {
		t.Fatalf("set stale: %v", err)
	}
	if err := store.Set(t.Context(), "ns", "fresh", json.RawMessage(`2`), now.Add(time.Hour)); err != nil {
		t.Fatalf("set fresh: %v", err)
	}
	if err := store.Set(t.Context(), "ns", "forever", json.RawMessage(`3`), time.Time{}); err != nil {
		t.Fatalf("set forever: %v", err)
	}

	removed, err := store.PurgeExpired(t.Context(), now)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed = %d, want 1", removed)
	}
	if _, found, err := store.Get(t.Context(), "ns", "fresh"); err != nil || !found {
		t.Errorf("purge took an unexpired entry: found=%v err=%v", found, err)
	}
	if _, found, err := store.Get(t.Context(), "ns", "forever"); err != nil || !found {
		t.Errorf("purge took an entry with no expiry: found=%v err=%v", found, err)
	}
}

var _ Reaper = (*MemoryStore)(nil)
