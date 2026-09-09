package kvstore_test

import (
	"testing"

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
