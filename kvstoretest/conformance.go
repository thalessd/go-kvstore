// Package kvstoretest pins the kvstore.Store contract. The same suite runs
// against every implementation, so two stores cannot quietly disagree about
// what Store means. A new backend is not done until it passes.
package kvstoretest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/thalessd/go-kvstore"
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return raw
}

func assertJSON(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	if string(got) != want {
		t.Errorf("value = %s, want %s", got, want)
	}
}

// Conformance exercises the contract documented on kvstore.Store: miss is
// (nil, false, nil), Set upserts, Delete is idempotent, expiry hides entries,
// and a Clear never leaves its namespace. It takes a Reaper so the sweep is
// covered too.
func Conformance(t *testing.T, store kvstore.Reaper) {
	t.Helper()
	ctx := t.Context()

	t.Run("miss is not an error", func(t *testing.T) {
		raw, found, err := store.Get(ctx, "conf", "absent")
		if found || err != nil || raw != nil {
			t.Fatalf("Get missing key: raw=%s found=%v err=%v", raw, found, err)
		}
	})

	t.Run("set get overwrite", func(t *testing.T) {
		if err := store.Set(ctx, "conf", "roundtrip", mustJSON(t, "v1"), time.Time{}); err != nil {
			t.Fatalf("set: %v", err)
		}
		raw, found, err := store.Get(ctx, "conf", "roundtrip")
		if err != nil || !found {
			t.Fatalf("get after set: found=%v err=%v", found, err)
		}
		assertJSON(t, raw, `"v1"`)

		if err := store.Set(ctx, "conf", "roundtrip", mustJSON(t, "v2"), time.Time{}); err != nil {
			t.Fatalf("overwrite: %v", err)
		}
		raw, _, err = store.Get(ctx, "conf", "roundtrip")
		if err != nil {
			t.Fatalf("get after overwrite: %v", err)
		}
		assertJSON(t, raw, `"v2"`)
	})

	t.Run("has", func(t *testing.T) {
		if err := store.Set(ctx, "conf", "present", mustJSON(t, true), time.Time{}); err != nil {
			t.Fatalf("set: %v", err)
		}
		if ok, err := store.Has(ctx, "conf", "present"); err != nil || !ok {
			t.Errorf("Has existing: ok=%v err=%v", ok, err)
		}
		if ok, err := store.Has(ctx, "conf", "absent"); err != nil || ok {
			t.Errorf("Has missing: ok=%v err=%v", ok, err)
		}
	})

	t.Run("delete is idempotent", func(t *testing.T) {
		if err := store.Set(ctx, "conf", "doomed", mustJSON(t, 1), time.Time{}); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := store.Delete(ctx, "conf", "never-existed"); err != nil {
			t.Errorf("delete missing key: %v", err)
		}
		if err := store.Delete(ctx, "conf", "doomed", "also-missing"); err != nil {
			t.Errorf("bulk delete with missing keys: %v", err)
		}
		if _, found, err := store.Get(ctx, "conf", "doomed"); err != nil || found {
			t.Errorf("get after delete: found=%v err=%v", found, err)
		}
	})

	t.Run("namespaces are isolated", func(t *testing.T) {
		if err := store.Set(ctx, "conf-a", "same", mustJSON(t, "a"), time.Time{}); err != nil {
			t.Fatalf("set a: %v", err)
		}
		if err := store.Set(ctx, "conf-b", "same", mustJSON(t, "b"), time.Time{}); err != nil {
			t.Fatalf("set b: %v", err)
		}
		raw, found, err := store.Get(ctx, "conf-a", "same")
		if err != nil || !found {
			t.Fatalf("get a: found=%v err=%v", found, err)
		}
		assertJSON(t, raw, `"a"`)

		if err := store.Delete(ctx, "conf-a", "same"); err != nil {
			t.Fatalf("delete a: %v", err)
		}
		if _, found, err := store.Get(ctx, "conf-b", "same"); err != nil || !found {
			t.Errorf("delete in one namespace reached another: found=%v err=%v", found, err)
		}
	})

	t.Run("entry written already expired is invisible", func(t *testing.T) {
		past := time.Now().Add(-time.Second)
		if err := store.Set(ctx, "conf", "expired", mustJSON(t, "stale"), past); err != nil {
			t.Fatalf("set expired: %v", err)
		}
		if raw, found, err := store.Get(ctx, "conf", "expired"); found || err != nil || raw != nil {
			t.Errorf("Get expired: raw=%s found=%v err=%v", raw, found, err)
		}
		if ok, err := store.Has(ctx, "conf", "expired"); err != nil || ok {
			t.Errorf("Has expired: ok=%v err=%v", ok, err)
		}
	})

	t.Run("clear stays inside its namespace", func(t *testing.T) {
		if err := store.Set(ctx, "conf-clear", "a", mustJSON(t, 1), time.Time{}); err != nil {
			t.Fatalf("set a: %v", err)
		}
		if err := store.Set(ctx, "conf-clear", "b", mustJSON(t, 2), time.Time{}); err != nil {
			t.Fatalf("set b: %v", err)
		}
		if err := store.Set(ctx, "conf-survivor", "x", mustJSON(t, 3), time.Time{}); err != nil {
			t.Fatalf("set survivor: %v", err)
		}

		if err := store.Clear(ctx, "conf-clear"); err != nil {
			t.Fatalf("clear: %v", err)
		}
		if _, found, err := store.Get(ctx, "conf-clear", "a"); err != nil || found {
			t.Errorf("cleared key survived: found=%v err=%v", found, err)
		}
		if _, found, err := store.Get(ctx, "conf-survivor", "x"); err != nil || !found {
			t.Errorf("clear reached another namespace: found=%v err=%v", found, err)
		}
	})

	// A store is a byte channel, not a JSON document store: a caller that
	// hashes or signs what it wrote has to read back the same bytes. A backend
	// that parses the value normalizes key order and whitespace, and the
	// scalars the subtests above use survive that unchanged — so an object is
	// the only fixture that catches it.
	t.Run("value round-trips byte for byte", func(t *testing.T) {
		exact := json.RawMessage(`{"zeta":1,  "alpha":{"nested":true},"beta":[1,2,3]}`)
		if err := store.Set(ctx, "conf-exact", "doc", exact, time.Time{}); err != nil {
			t.Fatalf("set: %v", err)
		}
		raw, found, err := store.Get(ctx, "conf-exact", "doc")
		if err != nil || !found {
			t.Fatalf("get: found=%v err=%v", found, err)
		}
		assertJSON(t, raw, string(exact))
	})

	// Runs last, and asserts a lower bound rather than an exact count: the
	// subtests above leave expired entries behind on purpose.
	t.Run("purge reclaims only what expired", func(t *testing.T) {
		// Set takes an arbitrary expiry moment, so a stale entry is written
		// rather than waited for.
		if err := store.Set(ctx, "conf-purge", "stale", mustJSON(t, "old"), time.Now().Add(-time.Minute)); err != nil {
			t.Fatalf("set stale: %v", err)
		}
		if err := store.Set(ctx, "conf-purge", "fresh", mustJSON(t, "new"), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("set fresh: %v", err)
		}

		removed, err := store.PurgeExpired(ctx, time.Now())
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		if removed < 1 {
			t.Errorf("removed = %d, want at least 1", removed)
		}
		if ok, err := store.Has(ctx, "conf-purge", "stale"); err != nil || ok {
			t.Errorf("expired entry survived the purge: ok=%v err=%v", ok, err)
		}
		if _, found, err := store.Get(ctx, "conf-purge", "fresh"); err != nil || !found {
			t.Errorf("purge took an unexpired entry: found=%v err=%v", found, err)
		}
	})
}
