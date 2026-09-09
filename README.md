# kvstore

[![Go Reference](https://pkg.go.dev/badge/github.com/thalessd/go-kvstore.svg)](https://pkg.go.dev/github.com/thalessd/go-kvstore)

A namespaced key-value store for Go with pluggable backends. Values are JSON, keys are scoped by a namespace, and every entry may carry an expiry moment.

- **Two backends included** — an in-process `MemoryStore` and a durable `Postgres` store.
- **One contract, verified** — `kvstoretest.Conformance` is a runnable suite every backend must pass, so two stores cannot quietly disagree about what `Store` means.
- **Byte-transparent** — `Get` returns exactly the bytes `Set` was given. The value column is `TEXT`, not `JSONB`, so nothing reorders your object's keys or strips its whitespace.
- **The in-memory store can be bounded** — `WithMaxEntriesPerNamespace` caps each namespace independently, so one namespace under load cannot evict another's entries.
- **A typed layer on top** — `Cache[T]` binds a namespace and a default TTL and handles the JSON, leaving `Store` codec-free.
- **The library owns its schema** — the Postgres store creates and manages its own PostgreSQL schema, so the application embedding it carries no migration for someone else's table.
- **No dependencies in production code** — the standard library only. The driver and the mock are test-only.

```sh
go get github.com/thalessd/go-kvstore
```

Requires Go 1.26 or later.

## Usage

```go
import "github.com/thalessd/go-kvstore"
```

### In-memory

```go
store := kvstore.NewMemory()

err := store.Set(ctx, "sessions", "abc", json.RawMessage(`{"user":"1"}`), time.Now().Add(time.Hour))
raw, found, err := store.Get(ctx, "sessions", "abc")
```

#### Bounding it

`NewMemory()` is **unbounded**, and that is worth understanding before relying on it: reads
evict expired entries lazily and a sweep reclaims them, but *only what expired*. A namespace
written with no expiry — which is what `Cache[T].Set(ctx, key, value, 0)` does — grows without
limit.

`WithMaxEntriesPerNamespace` caps it:

```go
store := kvstore.NewMemory(kvstore.WithMaxEntriesPerNamespace(10_000))
```

The cap is **per namespace**, each with its own recency list, so a flood in one namespace cannot
evict another's entries. The ceiling on the whole store is therefore the cap times the number of
namespaces, which your application can compute because its namespaces are constants. A count of
zero or less means unbounded.

Eviction is least-recently-used: a read counts as a use and moves the entry to the front, so an
entry you keep reading survives even if you never rewrite it. `Has` does not — a probe is not a
use.

The bound costs something on the read path, because promoting an entry writes to the recency
list and therefore takes the write lock, while an unbounded store serves reads under a read
lock. On a 12th-gen i7 with `-cpu 8` that is roughly 509 ns/op bounded against 244 ns/op
unbounded; `go test -bench BenchmarkMemoryGet` measures it on your own hardware. Both are far
cheaper than the database round trip a cache exists to avoid.

### PostgreSQL

The store owns its schema. Point it at a `*sql.DB` and let it prepare the DDL:

```go
store, err := kvstore.NewPostgres(db, kvstore.WithSchema("kvstore"))
if err != nil {
    return err
}

// Idempotent, so every replica may call it at boot.
if err := store.EnsureSchema(ctx); err != nil {
    return err
}
```

That creates `kvstore.entries` plus a partial index on `expires`. Nothing else in your database is touched, and every statement the store issues names the relation fully qualified — so it shares a connection pool with your application without depending on its `search_path`.

The table follows [`@keyv/postgres`](https://github.com/jaredwray/keyv/tree/main/storage/postgres) v6:

```sql
CREATE TABLE kvstore.entries (
    namespace  VARCHAR(255) NOT NULL,
    key        VARCHAR(255) NOT NULL,
    value      TEXT         NOT NULL,
    expires    BIGINT       NULL,      -- millisecond epoch, NULL means never
    PRIMARY KEY (namespace, key)
)
```

You never write this DDL, and you should not depend on it — but the widths are worth knowing: a key derived from something unbounded, like a URL or a query string, has to be hashed to fit 255 characters.

Reads filter expiry themselves, so nothing depends on a sweep being prompt. Call `PurgeExpired` from a cleanup ticker to keep the table from growing without bound:

```go
removed, err := store.PurgeExpired(ctx, time.Now())
```

### The typed cache

```go
type Session struct {
    UserID string `json:"user_id"`
}

sessions := kvstore.NewCache[Session](store, "sessions", 30*time.Minute)

err := sessions.Set(ctx, id, Session{UserID: "1"})   // default TTL
err = sessions.Set(ctx, id, Session{UserID: "1"}, 0) // explicit: never expires

session, found, err := sessions.Get(ctx, id)
err = sessions.Delete(ctx, id)
```

### The entry, with its expiry

`Get` answers with the bytes. `GetEntry` answers with the bytes *and* the moment they expire, which is what a caller copying an entry into another store needs — without it the copy has to invent a TTL, and one that outlives the original serves what the original has already forgotten:

```go
entry, found, err := store.GetEntry(ctx, "sessions", id)
if found {
    // entry.Value is the bytes; entry.Expires is zero when the entry never expires.
    err = other.Set(ctx, "sessions", id, entry.Value, entry.Expires)
}
```

`GetEntry` filters expiry exactly as `Get` does, so an expired entry is a miss and a copy can never revive one. The moment comes back to within a millisecond of what `Set` was given and never later than it — the Postgres store keeps a millisecond epoch — so compare it with `Equal` and a tolerance, not `==`.

## Configuration

| Option | Default | What it does |
| :--- | :--- | :--- |
| `WithSchema(name)` | `"kvstore"` | The PostgreSQL schema the store reads, writes and creates |
| `WithTable(name)` | `"entries"` | The table name, for a store sharing a schema with something else |
| `WithPersistence(p)` | `kvstore.Logged` | The table's durability. `kvstore.Unlogged` buys write speed by giving up crash-safety: the table is truncated after a crash, is invisible on a standby and is never replicated, so it is only sensible for a pure cache |

These configure the Postgres store's physical layout; the in-memory store's own option is
[`WithMaxEntriesPerNamespace`](#bounding-it).

Names are validated — lower-case ASCII, digits and underscore, not starting with a digit, at most 63 bytes — and quoted. `NewPostgres` returns an error on anything else rather than falling back to a default, because a typo would otherwise send every write to the wrong relation.

### Persistence is asserted, not just applied

`EnsureSchema` creates the table with the declared persistence *and then reads
`pg_class` back to check it*. That is not belt and braces: `CREATE ... IF NOT
EXISTS` silently ignores the persistence clause on a table that already exists,
so without the check a declaration that drifted from its table would never be
heard — including the dangerous direction, an `UNLOGGED` table under an
application that believes its writes survive a crash.

A drift returns a `*PersistenceMismatchError`, which `errors.As` picks out for a
caller that would rather log it than fail. The check is the last step, so the
schema, table and index all exist by then either way.

Converting an existing table is deliberate and belongs to the operator, because
the two ways out cost different things:

```go
// Cheap, and discards every entry. The conversion for a cache.
err := kvstore.RecreateTable(ctx, db, kvstore.WithPersistence(kvstore.Unlogged))
```

```sql
-- Keeps the entries, at the price of a full table rewrite under ACCESS EXCLUSIVE.
ALTER TABLE "kvstore"."entries" SET UNLOGGED;
```

Neither ever runs at boot: `EnsureSchema` is called by every replica, and a
rewrite there would freeze the table for the whole fleet.

`WithUnlogged()` is deprecated and means `WithPersistence(kvstore.Unlogged)`.

## The Store contract

Any type satisfying `Store` must honour all of this, and `kvstoretest.Conformance` checks it:

1. A `Get` miss returns `(nil, false, nil)` — absence is not an error.
2. `Set` is an upsert: writing an existing key overwrites it.
3. `Delete` is idempotent: deleting keys that do not exist is not an error.
4. A non-zero expiry moment expires: reads stop seeing the entry, and a best-effort reclamation follows.
5. `Clear` never leaves its namespace.
6. `Get` returns the bytes `Set` was given, unchanged — a backend must not reformat the value.
7. `GetEntry` answers with the value **and its expiry moment**, filtering expiry exactly as `Get` does — an expired entry is a miss, so copying an entry into another store cannot revive one. `Entry.Expires` is zero when the entry never expires, and `Get` and `GetEntry` agree on both visibility and bytes.
8. `Entry.Expires` is **the moment that store stops serving the entry**, and is never later than what `Set` was given. A leaf backend reports what it was given, give or take a millisecond it may truncate to, and zero stays zero; a store layered over another reports its own shorter horizon instead, including where `Set` was given zero. Compare with `Equal` and a tolerance rather than `==`, and read zero as no limit.

The bytes `Get` and `GetEntry` return are **read-only**: a backend may hand the same slice to concurrent callers, so a caller that mutates or appends copies first. That, an empty namespace or key, and one longer than 255 characters are caller preconditions rather than validated inputs.

```go
func TestMyStore(t *testing.T) {
    kvstoretest.Conformance(t, NewMyStore())
}
```

## Supported drivers

`Delete` passes a `[]string` to `key = ANY($2)`, so the driver must encode a Go slice as a PostgreSQL array. [`pgx/v5/stdlib`](https://github.com/jackc/pgx) does, and is what the test suite runs against. `lib/pq` requires `pq.Array` and is not supported as-is.

`EnsureSchema` needs `CREATE` on the database. An application whose role cannot have it should run the DDL as a separate step.

## Development

```sh
make check            # vet + gofmt
make test             # unit tests. No container runtime needed
make db-up            # PostgreSQL on :55433
make test-integration # contract tests against a real PostgreSQL
make db-down
```

## Credits

`kvstore` follows the shape of two libraries:

- **[keyv](https://github.com/jaredwray/keyv)** (Node.js) — namespaced keys, pluggable stores and a TTL per entry.
- **[philippgille/gokv](https://github.com/philippgille/gokv)** (Go) — one `Store` interface every backend implements, with JSON as the value codec.

Both were the basis for this library. It is a port of neither: **the API** takes an expiry `time.Time` rather than a millisecond count, every method carries a `context.Context`, and the typed generic `Cache[T]` sits on top of a codec-free `Store`. The Postgres **table** is keyv's, down to the millisecond epoch the moment is stored as — the divergence is in the Go surface, not in the storage.

## Upgrading to v0.4.0

`Store` gained a method: `GetEntry(ctx, namespace, key) (Entry, bool, error)`. The table is unchanged, so there is nothing to migrate.

Both bundled stores implement it. A backend of your own does not, and the compiler says so — the break is a build failure, never a wrong answer at runtime. A type that **embeds** `kvstore.Store` inherits the method and needs no change; one that implements the five by hand needs a sixth:

```go
func (s *MyStore) GetEntry(ctx context.Context, namespace, key string) (kvstore.Entry, bool, error) {
    // Same expiry filtering as Get: an expired entry is a miss.
}
```

`kvstoretest.Conformance` covers the new method, so a backend that passes the suite is done.

## Upgrading to v0.2.0

The Go API is unchanged; the table is not. `value` went from `JSONB` to `TEXT`, `expires_at TIMESTAMPTZ` became `expires BIGINT` in milliseconds, `created_at` / `updated_at` are gone, and `namespace` / `key` are capped at 255 characters.

**There is no migration.** Drop the schema and let `EnsureSchema` rebuild it at boot:

```sql
DROP SCHEMA IF EXISTS kvstore CASCADE;
```

If you forget, you get an error rather than corruption: `expires_at` no longer exists under that name, so a surviving `v0.1.0` table fails the first `Get` with `42703 column "expires" does not exist`, and no write lands in the old shape.

## License

[BSD-3-Clause](LICENSE)
