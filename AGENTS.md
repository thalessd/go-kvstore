# AGENTS.md

## Project Overview

`kvstore` is a namespaced key-value store for Go with pluggable backends. Values are JSON, keys are scoped by a namespace, and every entry may carry an expiry moment. Two backends ship with it:

- **`MemoryStore`** — in-process and per-replica. Reads hide expired entries lazily; a full sweep is amortized over writes. Unbounded by default; `WithMaxEntriesPerNamespace` adds a per-namespace LRU.
- **`Postgres`** — durable and shared. It owns its own schema, creates its own DDL, and never touches the host application's tables. The table follows `@keyv/postgres` v6: `namespace` and `key` are `VARCHAR(255)`, `value` is `TEXT`, and `expires` is a `BIGINT` millisecond epoch behind a partial index.

**A value is opaque bytes, not a document.** `value` is `TEXT` rather than `JSONB` precisely so that `Get` returns the bytes `Set` was given — a parsing column reorders object keys and drops whitespace, which made the two backends disagree about what `Store` means. Nothing here reads inside the value, so the parse bought nothing and cost a rewrite of every write.

On top of the codec-free `Store` sits `Cache[T]`, a typed layer that binds a namespace and a default TTL and marshals values for you.

**Production code depends on the standard library alone.** The driver, the mock and anything else live behind test build tags.

---

## Design Lineage

`kvstore` follows the shape of two libraries:

- **[keyv](https://github.com/jaredwray/keyv)** (Node.js) — namespaced keys, pluggable stores and a TTL per entry.
- **[philippgille/gokv](https://github.com/philippgille/gokv)** (Go) — one `Store` interface every backend implements, with JSON as the value codec.

Both were the basis for this library. It is a port of neither: **the API** takes an expiry `time.Time` rather than a millisecond count, every method carries a `context.Context`, and the typed generic `Cache[T]` sits on top of a `Store` that knows nothing about codecs.

The Postgres **table**, on the other hand, is keyv's — column names, widths, `TEXT` value and the millisecond epoch included. The divergence is in the Go surface, not in the storage: `time.Time` is what a Go caller should hold, and `Set` converts at the boundary.

---

## Layout

```text
kvstore.go                     Package doc, Store, Entry, Purger, Reaper
options.go                     Option, WithSchema / WithTable / WithPersistence
ident.go                       Identifier validation and quoting
memory.go                      MemoryStore, MemoryOption, the per-namespace LRU
postgres.go                    DBTX, Postgres, per-instance statements
schema.go                      (*Postgres).EnsureSchema, RecreateTable, DropSchema
cache.go                       Cache[T]
kvstoretest/conformance.go     The Store contract, as a runnable suite
helpers_test.go                Shared test helpers (sqlmock, JSON fixtures)
schema_test.go                 DDL and the persistence check, against sqlmock
postgres_integration_test.go   //go:build integration — real PostgreSQL
compose.yaml                   PostgreSQL for the integration tier, on :55433
```

---

## Commands

```sh
make check            # go vet (both tags) + gofmt check
make test             # unit tests with cover. No container runtime needed
make test-race        # race detector
make db-up            # PostgreSQL on :55433 (compose.yaml)
make test-integration # contract tests against a real PostgreSQL
make db-down
make tidy             # go mod tidy, failing if it was not already tidy
```

---

## Public API

| Symbol | What it is |
| :--- | :--- |
| `Store` | `Get` / `GetEntry` / `Set` / `Delete` / `Has` / `Clear` — the contract every backend implements |
| `Entry` | `Value json.RawMessage` + `Expires time.Time`; what `GetEntry` answers with, and what a caller needs to copy an entry into another store |
| `Purger` | `PurgeExpired(ctx, now) (int64, error)` — the explicit sweep |
| `Reaper` | `Store` + `Purger`; what a long-lived application holds |
| `DBTX` | What the Postgres store needs from a handle; `*sql.DB` and `*sql.Tx` satisfy it |
| `NewMemory(opts...) *MemoryStore` | In-process store; unbounded unless an option says otherwise |
| `MemoryOption` | Configures a `MemoryStore`; separate from `Option`, which is the Postgres layout |
| `WithMaxEntriesPerNamespace(n)` | Caps each namespace independently with an LRU. Zero or less is unbounded |
| `NewPostgres(db, opts...) (*Postgres, error)` | Durable store; fails on an unusable schema or table name |
| `(*Postgres).WithTx(tx)` | The same store bound to a transaction, layout carried over |
| `(*Postgres).EnsureSchema(ctx)` | Creates schema, table and index, then verifies the table's persistence. Idempotent |
| `RecreateTable(ctx, db, opts...)` | Drops and recreates the table at the declared persistence, discarding every entry |
| `DropSchema(ctx, db, opts...)` | Removes everything `EnsureSchema` created |
| `NewCache[T](store, namespace, ttl)` | Typed layer: `Get` / `Set(…, ttl…)` / `Delete` |
| `WithSchema` / `WithTable` / `WithPersistence` | Physical layout of the Postgres store |
| `Persistence` (`Logged` / `Unlogged`) | The table's durability, declared by the caller and asserted by `EnsureSchema` |
| `PersistenceMismatchError` | What the table is, what the store declares, and both ways to converge them |
| `DefaultSchema` / `DefaultTable` | `"kvstore"` and `"entries"` |
| `kvstoretest.Conformance(t, store)` | The contract suite |

### Quick start

```go
store, err := kvstore.NewPostgres(db, kvstore.WithSchema("kvstore"))
if err != nil {
    return err
}
if err := store.EnsureSchema(ctx); err != nil {
    return err
}

sessions := kvstore.NewCache[Session](store, "sessions", 30*time.Minute)
if err := sessions.Set(ctx, id, session); err != nil {
    return err
}
```

---

## The Store contract

Documented on `Store` and pinned by `kvstoretest.Conformance`:

1. **A `Get` miss returns `(nil, false, nil)`.** Absence is not an error.
2. **`Set` is an upsert.** Writing an existing key overwrites it.
3. **`Delete` is idempotent.** Deleting keys that do not exist is not an error.
4. **A non-zero expiry moment expires.** Reads stop seeing the entry, and a best-effort reclamation follows — lazy eviction or a sweep.
5. **`Clear` never leaves its namespace.**
6. **`Get` returns the bytes `Set` was given, unchanged.** A backend must not reformat the value. The conformance case that pins this uses an *object* with a deliberate key order and non-canonical whitespace, because scalars survive a JSON-parsing column untouched and would not catch the divergence.
7. **`GetEntry` answers with the value and its expiry moment**, filtering expiry exactly as `Get` does — an expired entry is a miss, so a caller propagating an entry to another store cannot revive one. `Entry.Expires` is zero when the entry never expires, and `Get` and `GetEntry` must agree on visibility and on bytes.
8. **`Entry.Expires` is the moment that store stops serving the entry**, and is never later than what `Set` was given. A leaf backend reports what it was given, give or take a millisecond it may truncate to — the Postgres store keeps a millisecond epoch and normalizes to UTC — and zero stays zero. A store layered over another reports its own shorter horizon instead, including where `Set` was given zero. Never later is the direction that matters, and the only one `assertMomentAtMost` checks: a moment read back as later than asked would let an entry outlive its window. The exact fidelity a leaf backend owes is pinned in that backend's tests, not in the suite.
9. **The bytes `Get` and `GetEntry` return are read-only.** A backend may hand the same slice to concurrent callers — the memory store does, and a tier serving from its L1 does — so a caller that mutates or appends copies first. Not testable, like the two preconditions below.
10. **Empty namespace and empty key are caller preconditions**, not validated.
11. **So is a namespace or key of at most 255 characters.** A backend may store them in a column that narrow, so a caller deriving a key from something unbounded — a URL, a query string — hashes it first. Not validated in Go: the Postgres store raises `22001` and the caller sees it.

**A new backend is not done until it passes `kvstoretest.Conformance`.** The suite takes a `Reaper`, so the sweep is covered too.

---

## Design rules

These are the invariants a change must not break.

- **The in-memory bound is per namespace, not global.** `cacheable`'s `lruSize` is a flat key count, and it can be: in keyv a namespace is a key *prefix* in one flat store. Here it is a real dimension — part of the primary key, and what `Clear` is scoped to — so the budget follows the model. It also buys the property that matters in a process running more than one cache: a flood in the rate limit's namespace cannot evict the OIDC client cache. The whole-store ceiling is the cap times the number of namespaces, which is computable because namespaces are application constants.
- **An unbounded `MemoryStore` allocates no recency list and reads under `RLock`.** Unbounded is the default, so it must not pay for the LRU. A bound means promoting on read, promoting writes to the list, and a write therefore needs `Lock` — measured at roughly 2x per read at `-cpu 8` in `BenchmarkMemoryGet`. `max` is immutable after the constructor, which is what lets the read paths branch on it without holding the lock, and `lookupLocked` is the single place the expiry check lives so the two paths cannot drift.
- **`Has` does not promote.** A probe is not a use; promoting on one would keep alive an entry nobody reads.
- **The sweep rebuilds, it does not delete key by key.** A Go map never releases its buckets, so a namespace that ballooned and then expired would hold the memory for good. The recency list is rebuilt in the same pass so survivors keep their order.
- **Production code imports the standard library only.** A driver, a mock or a container belongs behind a test build tag. `postgres.go` reaches PostgreSQL through `database/sql` and never names a driver.
- **Every statement names the relation fully qualified and quoted.** `search_path` is never consulted, so the store shares a connection pool with a host application safely.
- **Identifiers are validated, then quoted; nothing else is ever interpolated into SQL.** `validateIdent` is stricter than PostgreSQL — lower-case ASCII, digits and underscore, never leading with a digit, at most 63 bytes. Everything else in a statement is a `$n` parameter.
- **The statements are built once, in the constructor.** That is why `NewPostgres` returns an error instead of falling back to a default: a typo in a schema name would otherwise send every write to the wrong relation, silently.
- **`EnsureSchema` is a method, `DropSchema` is a free function.** Binding creation to the instance makes "the schema I created is the schema I read" true by construction. Destruction stays deliberately separate, and refuses `public`, because the drop is `CASCADE`.
- **`EnsureSchema` is idempotent and race-tolerant.** `IF NOT EXISTS` does not settle a concurrent first boot — PostgreSQL checks existence before taking the catalog lock — so a duplicate-object SQLSTATE is treated as success. The check asserts `interface{ SQLState() string }` structurally, which is how the package tolerates the race without depending on a driver.
- **"now" is always a Go-computed parameter.** No statement and no column reads the server's clock — there is no `DEFAULT now()` anywhere, and `epoch()` in `postgres.go` converts the moment at the boundary. A zero `time.Time` has to reach the column as `NULL`: `UnixMilli` of it is a large negative number, which would read as long expired.
- **One get statement, not two.** `GetEntry` is the primitive and `Get` delegates to it in both backends. Two statements — one selecting `value`, one selecting `value, expires` — would be free to drift in their `WHERE`, and a difference there is exactly the divergence contract item 7 forbids. The cost is one `BIGINT` scanned per `Get`.
- **The moment leaves the store in UTC.** `momentOf` normalizes what `time.UnixMilli` returns in the local zone, because `Cache[T]` hands `Set` a UTC moment and a round trip through the column must not change the zone. Pinned in the unit tier: the integration tier asserts no location.
- **The value column is `TEXT`, and stays `TEXT`.** `JSONB` would reformat what a caller stored, breaking contract item 6. `Get` scans into a `string` and converts, because `database/sql` assigns a driver string to `*[]byte` but not to a named slice type like `json.RawMessage`.
- **`Delete` passes a `[]string` to `key = ANY($2)`,** which requires a driver that encodes a Go slice as a PostgreSQL array. `pgx/v5/stdlib` does; `lib/pq` needs `pq.Array` and is therefore unsupported as-is.
- **`WithTx` clones the store.** A copy that lost its layout would write to the default relation while the original reads a configured one.
- **Persistence is a property of the deployed table, not of the code.** `EnsureSchema` creates the table with the declared persistence and then reads `pg_class.relpersistence` back, because `CREATE ... IF NOT EXISTS` ignores the clause on a table that already exists — without the check a flag that stopped matching its table is a silent no-op, `UNLOGGED` under an application expecting durability included. It never *converts*: `ALTER TABLE ... SET LOGGED/UNLOGGED` rewrites the whole table under `ACCESS EXCLUSIVE`, and a method every replica calls at boot is the worst place for that. The conversion is `RecreateTable`, run deliberately by the operator, and it discards the contents — which is what makes it cheaper than the rewrite, and legitimate only because an unlogged table is already truncated on crash.
- **The host application owns no DDL.** It supplies a schema name and calls `EnsureSchema`; it never carries a migration for this package's table.
- **The database role needs `CREATE` on the database** for `EnsureSchema` to work. An application that cannot grant that has to run the DDL as a separate step.

---

## Testing

Two tiers, and the split is deliberate: **`make test` must never need a container runtime.**

**Unit tier — sqlmock.** `newMockDB(t)` in `helpers_test.go` returns a `*sql.DB` plus the mock, and on cleanup fails the test when an expectation went unmet. It always installs `stringArrayConverter`, because sqlmock's option type is unexported and a variadic wrapper is impossible; the converter is permissive, so tests that do not need it are unaffected. Match statements with `regexp.QuoteMeta` against the fully qualified relation, and use `anyEpoch()` for the millisecond moment the store computes itself — it asserts the `int64`, so a parameter that stopped being an epoch fails there rather than reaching a `BIGINT` column as something else.

**Integration tier — real PostgreSQL.** `//go:build integration`, package `kvstore_test`. It reads `KVSTORE_TEST_DSN` and **skips cleanly when it is unset**, so `go test ./...` stays runnable without a server. Each case gets a schema of its own through `newSchema(t, name)`, which drops, creates and registers a cleanup — so cases cannot see each other's rows and each starts from virgin DDL. What belongs here: anything whose subject is real SQL semantics — that the DDL parses, that `EnsureSchema` is idempotent, that two schemas are genuinely isolated, that `PurgeExpired` counts exactly.

**The benchmark is documentation, not a gate.** `BenchmarkMemoryGet` exists because bounding the
store changes how reads lock, and that cost should be a number rather than a claim. Run it with
`-cpu 1,4,8`: the contention is the whole point, so a single-threaded figure hides it.

**Never sleep to test expiry.** `Set` takes an arbitrary expiry moment, so write one in the past. `MemoryStore` and `Cache[T]` also have an injectable `now` for white-box tests.

Test comments say why the test earns its place, never what the assertions do.

---

## Conventions

### Go

Match the language version in `go.mod`. Use `any`, not `interface{}`. Extract typed errors with `errors.AsType[E](err)`; compare sentinels with `errors.Is`. Prefer `slices.Contains`, builtin `min`/`max` and `for range n` over hand-rolled loops. In tests that have a `*testing.T`, pass `t.Context()`.

### Comments

Default to none. The code already says what it does; a comment earns its place only by recording a *why* the code cannot.

- **One to three lines.** Past that it probably belongs in this file instead.
- **State the reason, do not argue it.**
- **No history.** "Used to be", "until this existed" — git holds that.
- **Do not paraphrase the line below it.**

### Commits

Conventional Commits, with the description in **English**:

- **Format:** `<type>(<scope>): <short description in English>`
- **Types:** `feat`, `fix`, `refactor`, `docs`, `style`, `test`, `chore`, `perf`, `ci`, `build`
- **Guidelines:** imperative mood, lowercase, no trailing period, at most 72 characters on the first line. Prefer no body at all; add one only when the change genuinely needs explaining.
- **Never add a `Co-Authored-By` trailer.** A commit must have exactly one author.

---

## Compatibility

Semantic versioning, currently pre-1.0: the API may still change, and a breaking change ships as a `v0.x` bump rather than a new module path. Licensed under BSD-3-Clause.

### v0.4.0 — `Store` gained `GetEntry`

`Store` is six methods now: `GetEntry(ctx, namespace, key) (Entry, bool, error)` sits beside `Get` and answers with the value and its expiry moment. The table is unchanged, so there is nothing to migrate.

The break is a compile error, which is what makes it acceptable: a backend that does not implement the method fails to build rather than answering wrongly at runtime. A type embedding `kvstore.Store` inherits it and needs no change.

The method exists for a caller copying an entry from one store into another. Without the expiry moment that copy has to invent a TTL, and a copy that outlives its origin serves entries the origin has already forgotten.

### v0.2.0 — the Postgres table changed

The Go API is unchanged; the table is not. `value` went from `JSONB` to `TEXT`, `expires_at TIMESTAMPTZ` became `expires BIGINT` in milliseconds, `created_at` / `updated_at` are gone, and `namespace` / `key` are capped at 255 characters.

**There is no migration.** Drop the schema before upgrading and let `EnsureSchema` rebuild it:

```sql
DROP SCHEMA IF EXISTS kvstore CASCADE;
```

Forgetting is loud rather than silent, which is what makes dropping an acceptable upgrade path: `expires_at` no longer exists under that name, so a surviving `v0.1.0` table fails the first `Get` with `42703 column "expires" does not exist`. No write lands in the old shape.
