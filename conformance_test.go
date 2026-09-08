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
