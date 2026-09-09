package kvstore

import (
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// catalogRows is what persistenceQuery returns: relpersistence cast to text.
func catalogRows(relpersistence string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"relpersistence"}).AddRow(relpersistence)
}

func expectCreates(mock sqlmock.Sqlmock, createTable string) {
	mock.ExpectExec(sqlMatch(`CREATE SCHEMA IF NOT EXISTS "kvstore"`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(sqlMatch(createTable)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(sqlMatch(`CREATE INDEX IF NOT EXISTS "entries_expires_idx"`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectCatalog(mock sqlmock.Sqlmock, relpersistence string) {
	mock.ExpectQuery(sqlMatch("FROM pg_class")).
		WithArgs("kvstore", "entries").
		WillReturnRows(catalogRows(relpersistence))
}

// The persistence keyword goes between CREATE and TABLE, and the declared
// persistence is then checked against the catalog rather than assumed.
func TestEnsureSchemaCreatesAndVerifies(t *testing.T) {
	for _, tc := range []struct {
		name           string
		opts           []Option
		createTable    string
		relpersistence string
	}{
		{
			name:           "logged by default",
			createTable:    `CREATE TABLE IF NOT EXISTS ` + defaultRelation,
			relpersistence: "p",
		},
		{
			name:           "unlogged when declared",
			opts:           []Option{WithPersistence(Unlogged)},
			createTable:    `CREATE UNLOGGED TABLE IF NOT EXISTS ` + defaultRelation,
			relpersistence: "u",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			store := newStore(t, db, tc.opts...)

			expectCreates(mock, tc.createTable)
			expectCatalog(mock, tc.relpersistence)

			if err := store.EnsureSchema(t.Context()); err != nil {
				t.Fatalf("ensure schema: %v", err)
			}
		})
	}
}

// The drift this reports is the one CREATE ... IF NOT EXISTS swallows, so the
// error has to name both sides and survive errors.As.
func TestEnsureSchemaReportsPersistenceDrift(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db, WithPersistence(Unlogged))

	expectCreates(mock, `CREATE UNLOGGED TABLE IF NOT EXISTS `+defaultRelation)
	expectCatalog(mock, "p")

	err := store.EnsureSchema(t.Context())

	var mismatch *PersistenceMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("ensure schema: err = %v, want a *PersistenceMismatchError", err)
	}
	if mismatch.Want != Unlogged || mismatch.Got != Logged {
		t.Errorf("want=%s got=%s, want want=unlogged got=logged", mismatch.Want, mismatch.Got)
	}
	if mismatch.Relation != defaultRelation {
		t.Errorf("relation = %s, want %s", mismatch.Relation, defaultRelation)
	}
}

// A catalog read that fails is not a mismatch and must not be reported as
// one, nor swallowed into a successful boot.
func TestEnsureSchemaSurfacesACatalogFailure(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	expectCreates(mock, `CREATE TABLE IF NOT EXISTS `+defaultRelation)
	mock.ExpectQuery(sqlMatch("FROM pg_class")).
		WithArgs("kvstore", "entries").
		WillReturnRows(catalogRows("p").RowError(0, errors.New("boom")))

	if err := store.EnsureSchema(t.Context()); err == nil {
		t.Fatal("ensure schema: err = nil, want the catalog read to surface")
	}
}

// An empty catalog means the table went away between the CREATE and the read,
// which is a failure rather than a mismatch.
func TestEnsureSchemaRejectsAMissingTable(t *testing.T) {
	db, mock := newMockDB(t)
	store := newStore(t, db)

	expectCreates(mock, `CREATE TABLE IF NOT EXISTS `+defaultRelation)
	mock.ExpectQuery(sqlMatch("FROM pg_class")).
		WithArgs("kvstore", "entries").
		WillReturnRows(sqlmock.NewRows([]string{"relpersistence"}))

	err := store.EnsureSchema(t.Context())
	var mismatch *PersistenceMismatchError
	if err == nil || errors.As(err, &mismatch) {
		t.Fatalf("ensure schema: err = %v, want a plain failure", err)
	}
}

// The drop has to precede the create, and the create has to carry the
// persistence being converted to.
func TestRecreateTableDropsBeforeItCreates(t *testing.T) {
	db, mock := newMockDB(t)

	mock.ExpectExec(sqlMatch(`CREATE SCHEMA IF NOT EXISTS "kvstore"`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(sqlMatch(`DROP TABLE IF EXISTS ` + defaultRelation)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(sqlMatch(`CREATE UNLOGGED TABLE IF NOT EXISTS ` + defaultRelation)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(sqlMatch(`CREATE INDEX IF NOT EXISTS "entries_expires_idx"`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := RecreateTable(t.Context(), db, WithPersistence(Unlogged)); err != nil {
		t.Fatalf("recreate table: %v", err)
	}
}

func TestRecreateTableRejectsAnUnusableRelation(t *testing.T) {
	db, _ := newMockDB(t)

	if err := RecreateTable(t.Context(), db, WithSchema("Public")); err == nil {
		t.Error("recreate table: err = nil, want a rejected schema name")
	}
}
