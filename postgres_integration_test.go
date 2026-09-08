//go:build integration

package kvstore_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/thalessd/go-kvstore"
	"github.com/thalessd/go-kvstore/kvstoretest"
)

// dsnEnv names the database the integration tier runs against. An absent DSN
// is a clean skip rather than a failure, so `go test ./...` stays runnable
// without a server.
const dsnEnv = "KVSTORE_TEST_DSN"

func open(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set; skipping the integration tier", dsnEnv)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsnEnv, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", dsnEnv, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newSchema gives the test a schema of its own, so the suite's cases cannot
// see each other's rows and each starts from virgin DDL.
func newSchema(t *testing.T, name string, opts ...kvstore.Option) *kvstore.Postgres {
	t.Helper()

	db := open(t)
	opts = append([]kvstore.Option{kvstore.WithSchema(name)}, opts...)

	if err := kvstore.DropSchema(t.Context(), db, opts...); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
	store, err := kvstore.NewPostgres(db, opts...)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.EnsureSchema(t.Context()); err != nil {
		t.Fatalf("ensure %s: %v", name, err)
	}
	t.Cleanup(func() { _ = kvstore.DropSchema(t.Context(), db, opts...) })
	return store
}

func TestPostgresConformance(t *testing.T) {
	kvstoretest.Conformance(t, newSchema(t, "kvt_conformance"))
}

// Every replica calls EnsureSchema at boot, so the second call has to be a
// no-op against a database the first one already prepared.
func TestEnsureSchemaIsIdempotent(t *testing.T) {
	store := newSchema(t, "kvt_idempotent")

	for range 3 {
		if err := store.EnsureSchema(t.Context()); err != nil {
			t.Fatalf("ensure again: %v", err)
		}
	}

	if err := store.Set(t.Context(), "ns", "k", json.RawMessage(`1`), time.Time{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, found, err := store.Get(t.Context(), "ns", "k"); err != nil || !found {
		t.Errorf("get after re-ensure: found=%v err=%v", found, err)
	}
}

// The conformance suite asserts a lower bound because it shares a table; on
// virgin DDL the count is exact.
func TestPurgeExpiredCountsExactly(t *testing.T) {
	store := newSchema(t, "kvt_purge")
	ctx := t.Context()

	for _, key := range []string{"a", "b", "c"} {
		if err := store.Set(ctx, "ns", key, json.RawMessage(`1`), time.Now().Add(-time.Minute)); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	if err := store.Set(ctx, "ns", "keep", json.RawMessage(`1`), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("set keep: %v", err)
	}

	removed, err := store.PurgeExpired(ctx, time.Now())
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 3 {
		t.Errorf("removed = %d, want 3", removed)
	}
	if _, found, err := store.Get(ctx, "ns", "keep"); err != nil || !found {
		t.Errorf("purge took an unexpired entry: found=%v err=%v", found, err)
	}
}

// Two stores on the same database and the same key must not see each other:
// this is what makes a schema of the library's own an isolation boundary
// rather than a naming convention.
func TestSchemasAreIsolated(t *testing.T) {
	left := newSchema(t, "kvt_left")
	right := newSchema(t, "kvt_right")
	ctx := t.Context()

	if err := left.Set(ctx, "ns", "same", json.RawMessage(`"left"`), time.Time{}); err != nil {
		t.Fatalf("set left: %v", err)
	}
	if err := right.Set(ctx, "ns", "same", json.RawMessage(`"right"`), time.Time{}); err != nil {
		t.Fatalf("set right: %v", err)
	}

	raw, found, err := left.Get(ctx, "ns", "same")
	if err != nil || !found {
		t.Fatalf("get left: found=%v err=%v", found, err)
	}
	if string(raw) != `"left"` {
		t.Errorf("left read %s, want %q", raw, `"left"`)
	}

	if err := left.Clear(ctx, "ns"); err != nil {
		t.Fatalf("clear left: %v", err)
	}
	if ok, err := right.Has(ctx, "ns", "same"); err != nil || !ok {
		t.Errorf("a clear in one schema reached another: ok=%v err=%v", ok, err)
	}
}

// UNLOGGED goes before TABLE in the grammar, so only a real server proves
// the DDL parses.
func TestEnsureSchemaAcceptsUnlogged(t *testing.T) {
	store := newSchema(t, "kvt_unlogged", kvstore.WithUnlogged())

	if err := store.Set(t.Context(), "ns", "k", json.RawMessage(`1`), time.Time{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, found, err := store.Get(t.Context(), "ns", "k"); err != nil || !found {
		t.Errorf("get from an unlogged table: found=%v err=%v", found, err)
	}
}

// A custom table name has to reach the DDL as well as the statements.
func TestEnsureSchemaHonoursTheTableName(t *testing.T) {
	store := newSchema(t, "kvt_table", kvstore.WithTable("cache"))

	if err := store.Set(t.Context(), "ns", "k", json.RawMessage(`1`), time.Time{}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if ok, err := store.Has(t.Context(), "ns", "k"); err != nil || !ok {
		t.Errorf("has: ok=%v err=%v", ok, err)
	}
}

func TestDropSchemaRemovesTheRelation(t *testing.T) {
	db := open(t)
	opts := []kvstore.Option{kvstore.WithSchema("kvt_drop")}

	store, err := kvstore.NewPostgres(db, opts...)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.EnsureSchema(t.Context()); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := store.Set(t.Context(), "ns", "k", json.RawMessage(`1`), time.Time{}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := kvstore.DropSchema(t.Context(), db, opts...); err != nil {
		t.Fatalf("drop: %v", err)
	}

	// A missing relation is a driver error, not a miss: absence of a row and
	// absence of a table are different failures.
	if _, _, err := store.Get(t.Context(), "ns", "k"); err == nil {
		t.Error("Get succeeded after the schema was dropped")
	}
}

// public holds the host application's own tables and the drop is CASCADE.
func TestDropSchemaRefusesPublic(t *testing.T) {
	if err := kvstore.DropSchema(t.Context(), open(t), kvstore.WithSchema("public")); err == nil {
		t.Error("DropSchema accepted the public schema")
	}
}
