package kvstore

import (
	"database/sql"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The default relation, as every statement spells it: fully qualified and
// quoted, so search_path cannot redirect it.
const defaultRelation = `"kvstore"."entries"`

func sqlMatch(prefix string) string {
	return regexp.QuoteMeta(prefix)
}

func newStore(t *testing.T, db DBTX, opts ...Option) *Postgres {
	t.Helper()

	store, err := NewPostgres(db, opts...)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	return store
}

func TestPostgresGet(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	t.Run("hit", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"value", "expires"}).AddRow([]byte(`"v1"`), nil)
		mock.ExpectQuery(sqlMatch("SELECT value, expires FROM "+defaultRelation)).
			WithArgs("ns", "k1", anyEpoch()).
			WillReturnRows(rows)

		raw, found, err := store.Get(t.Context(), "ns", "k1")
		if err != nil || !found {
			t.Fatalf("get: found=%v err=%v", found, err)
		}
		assertJSON(t, raw, `"v1"`)
	})

	t.Run("miss is not an error", func(t *testing.T) {
		mock.ExpectQuery(sqlMatch("SELECT value, expires FROM "+defaultRelation)).
			WithArgs("ns", "absent", anyEpoch()).
			WillReturnRows(sqlmock.NewRows([]string{"value", "expires"}))

		raw, found, err := store.Get(t.Context(), "ns", "absent")
		if raw != nil || found || err != nil {
			t.Fatalf("miss: raw=%s found=%v err=%v", raw, found, err)
		}
	})
}

// The moment is what makes this store usable as the source a tier copies from,
// so it has to come back UTC and to the millisecond whatever zone the process
// runs in. The integration tier asserts no location, so this is the only place
// that pins it.
func TestPostgresGetEntry(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	t.Run("expiry comes back in UTC", func(t *testing.T) {
		expires := time.Date(2026, 9, 8, 13, 0, 0, 0, time.UTC)
		rows := sqlmock.NewRows([]string{"value", "expires"}).AddRow([]byte(`"v1"`), expires.UnixMilli())
		mock.ExpectQuery(sqlMatch("SELECT value, expires FROM "+defaultRelation)).
			WithArgs("ns", "k1", anyEpoch()).
			WillReturnRows(rows)

		entry, found, err := store.GetEntry(t.Context(), "ns", "k1")
		if err != nil || !found {
			t.Fatalf("get entry: found=%v err=%v", found, err)
		}
		assertJSON(t, entry.Value, `"v1"`)
		if !entry.Expires.Equal(expires) {
			t.Errorf("expires = %v, want %v", entry.Expires, expires)
		}
		if entry.Expires.Location() != time.UTC {
			t.Errorf("expires location = %v, want UTC", entry.Expires.Location())
		}
	})

	t.Run("a NULL expires is the zero moment", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"value", "expires"}).AddRow([]byte(`"v1"`), nil)
		mock.ExpectQuery(sqlMatch("SELECT value, expires FROM "+defaultRelation)).
			WithArgs("ns", "forever", anyEpoch()).
			WillReturnRows(rows)

		entry, found, err := store.GetEntry(t.Context(), "ns", "forever")
		if err != nil || !found {
			t.Fatalf("get entry: found=%v err=%v", found, err)
		}
		if !entry.Expires.IsZero() {
			t.Errorf("expires = %v, want zero", entry.Expires)
		}
	})
}

func TestPostgresSet(t *testing.T) {
	t.Run("with expiry", func(t *testing.T) {
		db, mock := newMockDB(t)
		store := newStore(t, db)

		expires := time.Date(2026, 9, 8, 13, 0, 0, 0, time.UTC)
		mock.ExpectExec(sqlMatch("INSERT INTO "+defaultRelation)).
			WithArgs("ns", "k1", `"v1"`, expires.UnixMilli()).
			WillReturnResult(sqlmock.NewResult(0, 1))

		if err := store.Set(t.Context(), "ns", "k1", json.RawMessage(`"v1"`), expires); err != nil {
			t.Fatalf("set: %v", err)
		}
	})

	// A zero expiry moment means "never", which reaches the driver as an
	// invalid NullInt64 rather than the epoch of the zero instant, which is a
	// large negative number and would read as long expired.
	t.Run("without expiry", func(t *testing.T) {
		db, mock := newMockDB(t)
		store := newStore(t, db)

		mock.ExpectExec(sqlMatch("INSERT INTO "+defaultRelation)).
			WithArgs("ns", "k1", `"v1"`, nil).
			WillReturnResult(sqlmock.NewResult(0, 1))

		if err := store.Set(t.Context(), "ns", "k1", json.RawMessage(`"v1"`), time.Time{}); err != nil {
			t.Fatalf("set: %v", err)
		}
	})
}

func TestPostgresDelete(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	mock.ExpectExec(sqlMatch("DELETE FROM "+defaultRelation)).
		WithArgs("ns", []string{"k1", "k2"}).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.Delete(t.Context(), "ns", "k1", "k2"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// No keys, no statement: sqlmock fails the test on unmet expectations,
	// so the absence of an expectation here is the assertion.
	if err := store.Delete(t.Context(), "ns"); err != nil {
		t.Fatalf("delete without keys: %v", err)
	}
}

func TestPostgresHas(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	mock.ExpectQuery(sqlMatch("SELECT EXISTS")).
		WithArgs("ns", "k1", anyEpoch()).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	ok, err := store.Has(t.Context(), "ns", "k1")
	if err != nil || !ok {
		t.Fatalf("has: ok=%v err=%v", ok, err)
	}
}

func TestPostgresClear(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	mock.ExpectExec(sqlMatch("DELETE FROM " + defaultRelation + " WHERE namespace = $1")).
		WithArgs("ns").
		WillReturnResult(sqlmock.NewResult(0, 3))

	if err := store.Clear(t.Context(), "ns"); err != nil {
		t.Fatalf("clear: %v", err)
	}
}

func TestPostgresPurgeExpired(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	mock.ExpectExec(sqlMatch("DELETE FROM " + defaultRelation + " WHERE expires")).
		WithArgs(anyEpoch()).
		WillReturnResult(sqlmock.NewResult(0, 7))

	removed, err := store.PurgeExpired(t.Context(), time.Now())
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 7 {
		t.Errorf("removed = %d, want 7", removed)
	}
}

// The configured layout has to reach the statements, not just the DDL.
func TestPostgresQualifiesTheConfiguredRelation(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db, WithSchema("kv_alt"), WithTable("cache"))

	mock.ExpectQuery(sqlMatch(`SELECT value, expires FROM "kv_alt"."cache"`)).
		WithArgs("ns", "k1", anyEpoch()).
		WillReturnRows(sqlmock.NewRows([]string{"value", "expires"}).AddRow([]byte(`1`), nil))

	if _, _, err := store.Get(t.Context(), "ns", "k1"); err != nil {
		t.Fatalf("get: %v", err)
	}
}

// WithTx must carry the layout, or a transactional write would land in the
// default relation while the original store reads a configured one.
func TestPostgresWithTxKeepsTheConfiguredRelation(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db, WithSchema("kv_alt"))

	mock.ExpectBegin()
	mock.ExpectExec(sqlMatch(`INSERT INTO "kv_alt"."entries"`)).
		WithArgs("ns", "k1", `1`, nil).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.WithTx(tx).Set(t.Context(), "ns", "k1", json.RawMessage(`1`), time.Time{}); err != nil {
		t.Fatalf("set in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestNewPostgresRejectsAnUnusableRelation(t *testing.T) {
	db, _ := newMockDB(t)

	for _, opt := range []Option{WithSchema("kv; drop"), WithTable("")} {
		if _, err := NewPostgres(db, opt); err == nil {
			t.Error("NewPostgres accepted an invalid identifier")
		}
	}
}

// The constructor is where a bad layout has to fail: a persistence nobody
// declared would otherwise reach the DDL as a keyword the store made up.
func TestNewPostgresRejectsAnUnknownPersistence(t *testing.T) {
	db, _ := newMockDB(t)

	if _, err := NewPostgres(db, WithPersistence(Persistence(7))); err == nil {
		t.Error("NewPostgres accepted a persistence outside the declared values")
	}
}

var _ Reaper = (*Postgres)(nil)
var _ DBTX = (*sql.DB)(nil)
var _ DBTX = (*sql.Tx)(nil)
