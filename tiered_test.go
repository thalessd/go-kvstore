package kvstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func newTier(t *testing.T, l1, l2 Store, ttl time.Duration) *TieredStore {
	t.Helper()

	tier, err := NewTiered(l1, l2, ttl)
	if err != nil {
		t.Fatalf("new tiered store: %v", err)
	}
	return tier
}

// countingStore turns a round-trip budget into an assertion. Only GetEntry and
// Set are counted, which is all the tier calls: a Get promoted from the
// embedded Store would not pass through here.
type countingStore struct {
	Store
	entryGets int
	sets      int
}

func (s *countingStore) GetEntry(ctx context.Context, namespace, key string) (Entry, bool, error) {
	s.entryGets++
	return s.Store.GetEntry(ctx, namespace, key)
}

func (s *countingStore) Set(ctx context.Context, namespace, key string, value json.RawMessage, expires time.Time) error {
	s.sets++
	return s.Store.Set(ctx, namespace, key, value, expires)
}

// failingStore stands in for a shared store that is down.
type failingStore struct {
	Store
	err error
}

func (s *failingStore) Set(context.Context, string, string, json.RawMessage, time.Time) error {
	return s.err
}

// blockingStore holds a read after it has already fetched the value, which is
// what makes the backfill race deterministic instead of timing-based. Writes
// pass straight through, so the same tier can write while a read is parked.
type blockingStore struct {
	Store
	entered chan<- struct{}
	release <-chan struct{}
}

func (s *blockingStore) GetEntry(ctx context.Context, namespace, key string) (Entry, bool, error) {
	entry, found, err := s.Store.GetEntry(ctx, namespace, key)
	s.entered <- struct{}{}
	<-s.release
	return entry, found, err
}

// The whole point of an l1: a hit must not cost anything downstream.
func TestTieredServesAHitWithoutTouchingL2(t *testing.T) {
	ctx := t.Context()
	l1 := NewMemory()
	l2 := &countingStore{Store: NewMemory()}
	tier := newTier(t, l1, l2, time.Minute)

	if err := tier.Set(ctx, "ns", "k", mustJSON(t, "v"), time.Time{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	writes := l2.sets

	for range 5 {
		if _, found, err := tier.Get(ctx, "ns", "k"); err != nil || !found {
			t.Fatalf("get: found=%v err=%v", found, err)
		}
	}

	if l2.entryGets != 0 {
		t.Errorf("hits in l1 reached l2 %d times", l2.entryGets)
	}
	if l2.sets != writes {
		t.Errorf("reads wrote to l2: %d writes, want %d", l2.sets, writes)
	}
}

// A miss has to reach l2 exactly once and leave a copy behind, or the tier is
// a pass-through with extra steps.
func TestTieredCopiesDownOnAMiss(t *testing.T) {
	ctx := t.Context()
	l1 := NewMemory()
	backing := NewMemory()
	if err := backing.Set(ctx, "ns", "k", mustJSON(t, "v"), time.Time{}); err != nil {
		t.Fatalf("seed l2: %v", err)
	}
	l2 := &countingStore{Store: backing}
	tier := newTier(t, l1, l2, time.Minute)

	for range 3 {
		raw, found, err := tier.Get(ctx, "ns", "k")
		if err != nil || !found {
			t.Fatalf("get: found=%v err=%v", found, err)
		}
		assertJSON(t, raw, `"v"`)
	}

	if l2.entryGets != 1 {
		t.Errorf("l2 was read %d times, want 1", l2.entryGets)
	}
	if ok, err := l1.Has(ctx, "ns", "k"); err != nil || !ok {
		t.Errorf("the miss left no copy in l1: ok=%v err=%v", ok, err)
	}
}

// Ordering the two layers' writes does not close this: the l2 read is already
// in flight when the write lands, so without the generation check the copy the
// backfill is carrying would overwrite the write in l1 and read as a lost one
// for a whole ttl.
func TestTieredDropsABackfillAWriteOvertook(t *testing.T) {
	ctx := t.Context()
	l1 := NewMemory()
	l2 := NewMemory()
	if err := l2.Set(ctx, "ns", "k", mustJSON(t, "old"), time.Time{}); err != nil {
		t.Fatalf("seed l2: %v", err)
	}

	entered, release := make(chan struct{}), make(chan struct{})
	tier := newTier(t, l1, &blockingStore{Store: l2, entered: entered, release: release}, time.Minute)

	done := make(chan error, 1)
	go func() {
		_, _, err := tier.GetEntry(ctx, "ns", "k")
		done <- err
	}()

	// The backfill has read "old" and is parked holding it.
	<-entered
	if err := tier.Set(ctx, "ns", "k", mustJSON(t, "new"), time.Time{}); err != nil {
		t.Fatalf("set while the backfill was in flight: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("get: %v", err)
	}

	entry, found, err := l1.GetEntry(ctx, "ns", "k")
	if err != nil || !found {
		t.Fatalf("l1 after the write: found=%v err=%v", found, err)
	}
	assertJSON(t, entry.Value, `"new"`)
}

// The ttl is the staleness window, so it has to win over a longer expiry and
// lose to a shorter one. An entry with no expiry at all must not land in l1 as
// "never": that is the case where staleness would be unbounded.
func TestTieredCapsTheMomentAtTheTtl(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2030, 9, 8, 12, 0, 0, 0, time.UTC)

	l1, l2 := NewMemory(), NewMemory()
	tier := newTier(t, l1, l2, time.Minute)
	tier.now = func() time.Time { return now }

	for _, tc := range []struct {
		name    string
		key     string
		expires time.Time
		want    time.Time
	}{
		{"an expiry beyond the ttl is capped", "far", now.Add(time.Hour), now.Add(time.Minute)},
		{"an expiry inside the ttl wins", "near", now.Add(time.Second), now.Add(time.Second)},
		{"no expiry becomes the ttl", "forever", time.Time{}, now.Add(time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := l2.Set(ctx, "ns", tc.key, mustJSON(t, "v"), tc.expires); err != nil {
				t.Fatalf("seed l2: %v", err)
			}

			entry, found, err := tier.GetEntry(ctx, "ns", tc.key)
			if err != nil || !found {
				t.Fatalf("get entry: found=%v err=%v", found, err)
			}
			if !entry.Expires.Equal(tc.want) {
				t.Errorf("reported expires = %v, want %v", entry.Expires, tc.want)
			}

			copied, found, err := l1.GetEntry(ctx, "ns", tc.key)
			if err != nil || !found {
				t.Fatalf("l1 copy: found=%v err=%v", found, err)
			}
			if !copied.Expires.Equal(tc.want) {
				t.Errorf("copy in l1 expires = %v, want %v", copied.Expires, tc.want)
			}
		})
	}
}

// The cap must not resurrect an entry written already expired.
func TestTieredSetInThePastStaysInvisible(t *testing.T) {
	ctx := t.Context()
	l1, l2 := NewMemory(), NewMemory()
	tier := newTier(t, l1, l2, time.Minute)

	if err := tier.Set(ctx, "ns", "k", mustJSON(t, "stale"), time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("set: %v", err)
	}

	for name, store := range map[string]Store{"tier": tier, "l1": l1, "l2": l2} {
		if _, found, err := store.Get(ctx, "ns", "k"); err != nil || found {
			t.Errorf("%s served an expired entry: found=%v err=%v", name, found, err)
		}
	}
}

// An l2 that refused the write must not leave l1 answering for it, which is the
// only reason the writes are ordered rather than concurrent.
func TestTieredSetLeavesL1AloneWhenL2Fails(t *testing.T) {
	ctx := t.Context()
	l1 := NewMemory()
	down := &failingStore{Store: NewMemory(), err: errors.New("l2 is down")}
	tier := newTier(t, l1, down, time.Minute)

	if err := tier.Set(ctx, "ns", "k", mustJSON(t, "v"), time.Time{}); err == nil {
		t.Fatal("a refused l2 write was reported as success")
	}
	if ok, err := l1.Has(ctx, "ns", "k"); err != nil || ok {
		t.Errorf("l1 holds what the shared store never received: ok=%v err=%v", ok, err)
	}
}

// The count that matters is l2's: it is the store whose table grows. Summing
// the layers would report a number that describes neither.
func TestTieredPurgeReportsTheL2Count(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	l1, l2 := NewMemory(), NewMemory()
	tier := newTier(t, l1, l2, time.Minute)

	// Two stale rows in l2, one of which also has a copy in l1.
	for _, key := range []string{"a", "b"} {
		if err := l2.Set(ctx, "ns", key, mustJSON(t, "v"), now.Add(-time.Minute)); err != nil {
			t.Fatalf("seed l2 %s: %v", key, err)
		}
	}
	if err := l1.Set(ctx, "ns", "a", mustJSON(t, "v"), now.Add(-time.Minute)); err != nil {
		t.Fatalf("seed l1: %v", err)
	}

	removed, err := tier.PurgeExpired(ctx, now)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 2 {
		t.Errorf("removed = %d, want 2 — the l2 count, not the sum", removed)
	}
	if l1.countLocked() != 0 {
		t.Error("l1 was not swept")
	}
}

// The ttl is the staleness window between replicas, so inheriting it by
// accident is the one mistake the constructor refuses.
func TestNewTieredRejectsWhatItCannotDefault(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		if _, err := NewTiered(NewMemory(), NewMemory(), ttl); err == nil {
			t.Errorf("ttl %s was accepted", ttl)
		}
	}
	if _, err := NewTiered(nil, NewMemory(), time.Minute); err == nil {
		t.Error("a nil l1 was accepted")
	}
	if _, err := NewTiered(NewMemory(), nil, time.Minute); err == nil {
		t.Error("a nil l2 was accepted")
	}
}

var _ Reaper = (*TieredStore)(nil)
