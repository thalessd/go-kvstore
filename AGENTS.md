# AGENTS.md

## Project Overview

`kvstore` is a namespaced key-value store for Go with pluggable backends. Values are JSON, keys are scoped by a namespace, and every entry may carry an expiry moment. Two backends ship with it:

- **`MemoryStore`** — in-process and per-replica. Reads hide expired entries lazily; a full sweep is amortized over writes.
- **`Postgres`** — durable and shared. It owns its own schema, creates its own DDL, and never touches the host application's tables.

On top of the codec-free `Store` sits `Cache[T]`, a typed layer that binds a namespace and a default TTL and marshals values for you.

**Production code depends on the standard library alone.** The driver, the mock and anything else live behind test build tags.

---

## Design Lineage

`kvstore` follows the shape of two libraries:

- **[keyv](https://github.com/jaredwray/keyv)** (Node.js) — namespaced keys, pluggable stores and a TTL per entry.
- **[philippgille/gokv](https://github.com/philippgille/gokv)** (Go) — one `Store` interface every backend implements, with JSON as the value codec.

Both were the basis for this library. It is a port of neither: expiry is a `time.Time` rather than a millisecond count, every method carries a `context.Context`, and the typed generic `Cache[T]` sits on top of a `Store` that knows nothing about codecs.

---

## Layout

```text
kvstore.go                     Package doc, Store, Purger, Reaper
options.go                     Option, WithSchema / WithTable / WithUnlogged
ident.go                       Identifier validation and quoting
memory.go                      MemoryStore
postgres.go                    DBTX, Postgres, per-instance statements
schema.go                      (*Postgres).EnsureSchema, DropSchema
cache.go                       Cache[T]
kvstoretest/conformance.go     The Store contract, as a runnable suite
helpers_test.go                Shared test helpers (sqlmock, JSON fixtures)
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
| `Store` | `Get` / `Set` / `Delete` / `Has` / `Clear` — the contract every backend implements |
| `Purger` | `PurgeExpired(ctx, now) (int64, error)` — the explicit sweep |
| `Reaper` | `Store` + `Purger`; what a long-lived application holds |
| `DBTX` | What the Postgres store needs from a handle; `*sql.DB` and `*sql.Tx` satisfy it |
| `NewMemory() *MemoryStore` | In-process store |
| `NewPostgres(db, opts...) (*Postgres, error)` | Durable store; fails on an unusable schema or table name |
| `(*Postgres).WithTx(tx)` | The same store bound to a transaction, layout carried over |
| `(*Postgres).EnsureSchema(ctx)` | Creates schema, table and index. Idempotent |
| `DropSchema(ctx, db, opts...)` | Removes everything `EnsureSchema` created |
| `NewCache[T](store, namespace, ttl)` | Typed layer: `Get` / `Set(…, ttl…)` / `Delete` |
| `WithSchema` / `WithTable` / `WithUnlogged` | Physical layout of the Postgres store |
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
6. **Empty namespace and empty key are caller preconditions**, not validated.

**A new backend is not done until it passes `kvstoretest.Conformance`.** The suite takes a `Reaper`, so the sweep is covered too.

---

## Design rules

These are the invariants a change must not break.

- **Production code imports the standard library only.** A driver, a mock or a container belongs behind a test build tag. `postgres.go` reaches PostgreSQL through `database/sql` and never names a driver.
- **Every statement names the relation fully qualified and quoted.** `search_path` is never consulted, so the store shares a connection pool with a host application safely.
- **Identifiers are validated, then quoted; nothing else is ever interpolated into SQL.** `validateIdent` is stricter than PostgreSQL — lower-case ASCII, digits and underscore, never leading with a digit, at most 63 bytes. Everything else in a statement is a `$n` parameter.
- **The statements are built once, in the constructor.** That is why `NewPostgres` returns an error instead of falling back to a default: a typo in a schema name would otherwise send every write to the wrong relation, silently.
- **`EnsureSchema` is a method, `DropSchema` is a free function.** Binding creation to the instance makes "the schema I created is the schema I read" true by construction. Destruction stays deliberately separate, and refuses `public`, because the drop is `CASCADE`.
- **`EnsureSchema` is idempotent and race-tolerant.** `IF NOT EXISTS` does not settle a concurrent first boot — PostgreSQL checks existence before taking the catalog lock — so a duplicate-object SQLSTATE is treated as success. The check asserts `interface{ SQLState() string }` structurally, which is how the package tolerates the race without depending on a driver.
- **"now" is always a Go-computed parameter.** `DEFAULT now()` appears only on `created_at` / `updated_at` bookkeeping.
- **`Delete` passes a `[]string` to `key = ANY($2)`,** which requires a driver that encodes a Go slice as a PostgreSQL array. `pgx/v5/stdlib` does; `lib/pq` needs `pq.Array` and is therefore unsupported as-is.
- **`WithTx` clones the store.** A copy that lost its layout would write to the default relation while the original reads a configured one.
- **The host application owns no DDL.** It supplies a schema name and calls `EnsureSchema`; it never carries a migration for this package's table.
- **The database role needs `CREATE` on the database** for `EnsureSchema` to work. An application that cannot grant that has to run the DDL as a separate step.

---

## Testing

Two tiers, and the split is deliberate: **`make test` must never need a container runtime.**

**Unit tier — sqlmock.** `newMockDB(t)` in `helpers_test.go` returns a `*sql.DB` plus the mock, and on cleanup fails the test when an expectation went unmet. It always installs `stringArrayConverter`, because sqlmock's option type is unexported and a variadic wrapper is impossible; the converter is permissive, so tests that do not need it are unaffected. Match statements with `regexp.QuoteMeta` against the fully qualified relation, and use `anyTime()` for a timestamp the store computes itself.

**Integration tier — real PostgreSQL.** `//go:build integration`, package `kvstore_test`. It reads `KVSTORE_TEST_DSN` and **skips cleanly when it is unset**, so `go test ./...` stays runnable without a server. Each case gets a schema of its own through `newSchema(t, name)`, which drops, creates and registers a cleanup — so cases cannot see each other's rows and each starts from virgin DDL. What belongs here: anything whose subject is real SQL semantics — that the DDL parses, that `EnsureSchema` is idempotent, that two schemas are genuinely isolated, that `PurgeExpired` counts exactly.

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
