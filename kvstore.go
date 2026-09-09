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
// neither: expiry is a time.Time in the API rather than a millisecond count,
// every method carries a context.Context, and the typed generic Cache[T] sits
// on top of a codec-free Store. The Postgres table itself follows keyv, down
// to the millisecond epoch the moment is stored as.
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

// Entry is a value with the metadata a caller needs to reproduce it in another
// store. Expires is zero when the entry never expires.
type Entry struct {
	Value   json.RawMessage
	Expires time.Time
}

// Store persists values under (namespace, key). Implementations must be safe
// for concurrent use.
//
// Contract, shared by every implementation and pinned by the conformance test:
//   - A Get miss returns (nil, false, nil) — absence is not an error.
//   - Set is an upsert: writing an existing key overwrites it.
//   - Delete is idempotent: deleting keys that do not exist is not an error.
//   - A value whose expires moment is not zero expires: reads stop seeing it
//     and a best-effort reclamation follows (lazy eviction or a sweep).
//   - Get returns the bytes Set was given, unchanged. A backend must not
//     reformat the value.
//   - GetEntry answers with the value and its expiry moment, filtering expiry
//     exactly as Get does: an expired entry is a miss, so a caller copying an
//     entry into another store cannot revive one. A miss returns the zero
//     Entry, and Get and GetEntry agree on both visibility and bytes.
//   - Entry.Expires is the moment this store stops serving the entry, and is
//     never later than what Set was given. A leaf backend reports what it was
//     given, give or take a millisecond it may truncate to, and zero stays
//     zero; a store layered over another reports its own shorter horizon
//     instead, including where Set was given zero. So compare the moment with
//     Equal and a tolerance rather than ==, and read zero as no limit.
//   - The bytes Get and GetEntry return are read-only. A backend may hand the
//     same slice to concurrent callers, so a caller that mutates or appends
//     copies first. A caller precondition, like the two that follow.
//   - Empty namespace and empty key are caller preconditions, not validated.
//   - So is a namespace or key of at most 255 characters. A backend may store
//     them in a column that narrow, so a caller deriving a key from something
//     unbounded — a URL, a query string — hashes it first.
type Store interface {
	Get(ctx context.Context, namespace, key string) (json.RawMessage, bool, error)
	GetEntry(ctx context.Context, namespace, key string) (Entry, bool, error)
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
