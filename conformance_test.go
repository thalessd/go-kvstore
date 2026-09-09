package kvstore_test

import (
	"testing"
	"time"

	"github.com/thalessd/go-kvstore"
	"github.com/thalessd/go-kvstore/kvstoretest"
)

// Runs from the external test package because the conformance suite imports
// kvstore, which the white-box memory tests cannot do without a cycle.
func TestMemoryConformance(t *testing.T) {
	kvstoretest.Conformance(t, kvstore.NewMemory())
}

// Bounding the store must not change what Store means. The bound is well above
// what the suite writes to any one namespace, because the contract promises
// nothing about retention: a tight bound would evict entries the suite still
// expects to read, and would be testing the eviction, not the contract.
func TestMemoryConformanceBounded(t *testing.T) {
	kvstoretest.Conformance(t, kvstore.NewMemory(kvstore.WithMaxEntriesPerNamespace(64)))
}

// A tier is a Store, so the contract holds for it too. Two ttls because they
// exercise different halves of the cap: the short one decides every copy's
// horizon, the long one leaves the l2 expiry to win. The suite must not be able
// to tell which it got.
func TestTieredConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"the ttl decides the horizon", time.Second},
		{"the l2 expiry decides it", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tier, err := kvstore.NewTiered(kvstore.NewMemory(), kvstore.NewMemory(), tc.ttl)
			if err != nil {
				t.Fatalf("new tiered store: %v", err)
			}
			kvstoretest.Conformance(t, tier)
		})
	}
}
