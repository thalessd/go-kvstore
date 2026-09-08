# kvstore

[![Go Reference](https://pkg.go.dev/badge/github.com/thalessd/go-kvstore.svg)](https://pkg.go.dev/github.com/thalessd/go-kvstore)

A namespaced key-value store for Go with pluggable backends. Values are JSON, keys are scoped by a namespace, and every entry may carry an expiry moment.

- **Two backends included** — an in-process `MemoryStore` and a durable `Postgres` store.
- **One contract, verified** — `kvstoretest.Conformance` is a runnable suite every backend must pass, so two stores cannot quietly disagree about what `Store` means.
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

That creates `kvstore.entries` plus a partial index on `expires_at`. Nothing else in your database is touched, and every statement the store issues names the relation fully qualified — so it shares a connection pool with your application without depending on its `search_path`.

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

## Configuration

| Option | Default | What it does |
| :--- | :--- | :--- |
| `WithSchema(name)` | `"kvstore"` | The PostgreSQL schema the store reads, writes and creates |
| `WithTable(name)` | `"entries"` | The table name, for a store sharing a schema with something else |
| `WithUnlogged()` | off | Creates the table `UNLOGGED`: faster writes, no crash-safety or replication. Only sensible for a pure cache |

Names are validated — lower-case ASCII, digits and underscore, not starting with a digit, at most 63 bytes — and quoted. `NewPostgres` returns an error on anything else rather than falling back to a default, because a typo would otherwise send every write to the wrong relation.

## The Store contract

Any type satisfying `Store` must honour all of this, and `kvstoretest.Conformance` checks it:

1. A `Get` miss returns `(nil, false, nil)` — absence is not an error.
2. `Set` is an upsert: writing an existing key overwrites it.
3. `Delete` is idempotent: deleting keys that do not exist is not an error.
4. A non-zero expiry moment expires: reads stop seeing the entry, and a best-effort reclamation follows.
5. `Clear` never leaves its namespace.

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

Both were the basis for this library. It is a port of neither: expiry is a `time.Time` rather than a millisecond count, every method carries a `context.Context`, and the typed generic `Cache[T]` sits on top of a codec-free `Store`.

## License

[BSD-3-Clause](LICENSE)
