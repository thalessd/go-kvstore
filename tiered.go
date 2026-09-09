package kvstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// TieredStore reads through l1 and falls back to l2, the store the fleet
// shares. It is a Store itself, so Cache[T] and anything else holding a Store
// composes over it unchanged.
//
// Writes reach l2 first and l1 second: an l2 that failed must not leave l1
// serving what the shared store never received. Errors from either layer
// propagate — this package returns them rather than swallowing them into an
// event, which is where it parts ways with cacheable.
//
// Entry.Expires from a tier is how long the answer is good for, not when the
// value dies: past that moment the tier reads l2 again. It is therefore
// shorter than what Set was given, and never zero.
//
// Two limits are worth knowing before wiring one up. A replica does not see
// another's Delete for up to the ttl, because l1 is per process and nothing
// here carries an invalidation between them. And there is no WithTx: to compose
// a kv write with other statements in one transaction, use the Postgres store
// directly.
type TieredStore struct {
	l1, l2 Store
	ttl    time.Duration

	// gen discards a backfill that a write overtook. See GetEntry and Set.
	gen atomic.Uint64

	now func() time.Time
}

// NewTiered returns a store reading through l1 and falling back to l2. It fails
// on a ttl of zero or less rather than reading it as "no limit": the ttl is the
// ceiling on how long one replica keeps serving what another has already
// deleted, and inheriting that by accident is the one mistake a tier must not
// allow.
//
// Nothing here can check that l1 is bounded, because l1 is only a Store. An
// unbounded one caches whatever anyone reads from l2 until the process dies, so
// pass NewMemory(WithMaxEntriesPerNamespace(n)).
func NewTiered(l1, l2 Store, ttl time.Duration) (*TieredStore, error) {
	if l1 == nil || l2 == nil {
		return nil, fmt.Errorf("kvstore: a tiered store needs both an l1 and an l2")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf(
			"kvstore: a tiered store needs a positive l1 ttl, got %s; it is the staleness window between replicas", ttl)
	}
	return &TieredStore{l1: l1, l2: l2, ttl: ttl, now: time.Now}, nil
}

// horizon is how long l1 may serve a copy: the entry's own expiry or the ttl,
// whichever comes first. A zero expires means the entry never expires, so the
// ttl decides; an expiry already past stays past, which is what keeps a stale
// entry invisible in l1 too.
func (s *TieredStore) horizon(expires time.Time) time.Time {
	capped := s.now().UTC().Add(s.ttl)
	if expires.IsZero() || capped.Before(expires) {
		return capped
	}
	return expires
}

func (s *TieredStore) Get(ctx context.Context, namespace, key string) (json.RawMessage, bool, error) {
	entry, found, err := s.GetEntry(ctx, namespace, key)
	return entry.Value, found, err
}

// GetEntry answers from l1, and on a miss reads l2 and copies the entry down.
//
// The copy is dropped when a write landed while l2 was being read. Without
// that, a slow backfill would put the pre-write value into l1, where it would
// outlive the write by up to the ttl and read as a lost write — ordering the
// two layers' writes does not close this, because the read is already in
// flight. The counter is deliberately coarse: a write to any key discards every
// backfill in flight, which is affordable because a tier only pays for itself
// under reads.
func (s *TieredStore) GetEntry(ctx context.Context, namespace, key string) (Entry, bool, error) {
	if entry, found, err := s.l1.GetEntry(ctx, namespace, key); err != nil || found {
		return entry, found, err
	}

	gen := s.gen.Load()
	entry, found, err := s.l2.GetEntry(ctx, namespace, key)
	if err != nil || !found {
		return Entry{}, false, err
	}

	// The horizon is reported whether or not the copy was kept, so the answer
	// does not depend on which layer served it.
	expires := s.horizon(entry.Expires)
	if s.gen.Load() == gen {
		// An l1 that refuses a write is broken in a way the caller should hear
		// about; degrading to "no cache" silently is the failure mode this
		// package refuses everywhere else.
		if err := s.l1.Set(ctx, namespace, key, entry.Value, expires); err != nil {
			return Entry{}, false, err
		}
	}
	return Entry{Value: entry.Value, Expires: expires}, true, nil
}

func (s *TieredStore) Set(ctx context.Context, namespace, key string, value json.RawMessage, expires time.Time) error {
	if err := s.l2.Set(ctx, namespace, key, value, expires); err != nil {
		return err
	}
	s.invalidate()
	return s.l1.Set(ctx, namespace, key, value, s.horizon(expires))
}

func (s *TieredStore) Delete(ctx context.Context, namespace string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := s.l2.Delete(ctx, namespace, keys...); err != nil {
		return err
	}
	s.invalidate()
	return s.l1.Delete(ctx, namespace, keys...)
}

func (s *TieredStore) Clear(ctx context.Context, namespace string) error {
	if err := s.l2.Clear(ctx, namespace); err != nil {
		return err
	}
	s.invalidate()
	return s.l1.Clear(ctx, namespace)
}

// invalidate runs between the two layers' writes, which is what makes the
// discard in GetEntry sound. A backfill either observes the bump and drops its
// copy, or its l1 write lands before the one that follows here and is
// overwritten by it. Bumping before the l2 write instead would leave the hole
// open: a backfill starting after the bump could still read the pre-write value.
func (s *TieredStore) invalidate() { s.gen.Add(1) }

// Has does not copy anything down: a probe is not a use.
func (s *TieredStore) Has(ctx context.Context, namespace, key string) (bool, error) {
	if found, err := s.l1.Has(ctx, namespace, key); err != nil || found {
		return found, err
	}
	return s.l2.Has(ctx, namespace, key)
}

// PurgeExpired reports what the sweep reclaimed in l2, because that is the
// count that matters: l2 is the store whose table grows without bound. l1 is
// swept too but its count is not added — the layers hold different populations,
// so a sum would mean nothing. A layer that is not a Purger is skipped, and if
// l2 is one of those the l1 count is reported instead.
func (s *TieredStore) PurgeExpired(ctx context.Context, now time.Time) (int64, error) {
	var removed int64
	reported := false

	if purger, ok := s.l2.(Purger); ok {
		n, err := purger.PurgeExpired(ctx, now)
		if err != nil {
			return 0, err
		}
		removed, reported = n, true
	}
	if purger, ok := s.l1.(Purger); ok {
		n, err := purger.PurgeExpired(ctx, now)
		if err != nil {
			return 0, err
		}
		if !reported {
			removed = n
		}
	}
	return removed, nil
}
