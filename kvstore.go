// Package kvstore is a namespaced key-value abstraction over pluggable
// stores, in the shape of the keyv and gokv libraries but idiomatic Go: every
// method takes a context, values are JSON, and a typed Cache[T] layer sits on
// top of the raw Store.
//
// The design follows two libraries. keyv (https://github.com/jaredwray/keyv)
// for Node.js contributes namespaced keys, pluggable stores and a TTL per
// entry; philippgille/gokv (https://github.com/philippgille/gokv) for Go
// contributes the single Store interface every backend implements, with JSON
// as the value codec. Both were the basis for this package. It is a port of
// neither: expiry is a time.Time rather than a millisecond count, every
// method carries a context.Context, and the typed generic Cache[T] sits on
// top of a codec-free Store.
//
// Production code here depends on the standard library alone. The Postgres
// store keeps its SQL as per-instance statements behind a local DBTX, and
// owns the DDL for the schema it reads, so a host application carries no
// migration for this package.
package kvstore

import (
	"context"
	"encoding/json"
	"time"
)

// Store persists values under (namespace, key). Implementations must be safe
// for concurrent use.
//
// Contract, shared by every implementation and pinned by the conformance test:
//   - A Get miss returns (nil, false, nil) — absence is not an error.
//   - Set is an upsert: writing an existing key overwrites it.
//   - Delete is idempotent: deleting keys that do not exist is not an error.
//   - A value whose expires moment is not zero expires: reads stop seeing it
//     and a best-effort reclamation follows (lazy eviction or a sweep).
//   - Empty namespace and empty key are caller preconditions, not validated.
type Store interface {
	Get(ctx context.Context, namespace, key string) (json.RawMessage, bool, error)
	Set(ctx context.Context, namespace, key string, value json.RawMessage, expires time.Time) error
	Delete(ctx context.Context, namespace string, keys ...string) error
	Has(ctx context.Context, namespace, key string) (bool, error)
	Clear(ctx context.Context, namespace string) error
}

// Purger reclaims already-expired entries with an explicit sweep. It is
// separate from Store on purpose: a backend with native expiry has nothing to
// do here, and a cleanup ticker is the only caller.
type Purger interface {
	PurgeExpired(ctx context.Context, now time.Time) (removed int64, err error)
}

// Reaper is what a long-lived application holds: the Store for the data path,
// the sweep for its cleanup ticker. Both bundled stores satisfy it.
type Reaper interface {
	Store
	Purger
}
