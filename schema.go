package kvstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
)

// Column widths, and the keyv defaults. Declaring them keeps a key derived
// from something attacker-controlled — a URL, a query string — from reaching
// the primary key's btree, whose index row cannot exceed 2704 bytes.
const (
	maxNamespaceLen = 255
	maxKeyLen       = 255
)

// The DDL is split in two halves rather than one list because RecreateTable
// slips a DROP between them; EnsureSchema and RecreateTable emit the same
// text either way, so the two cannot drift.
func (o options) createSchemaStatement() string {
	return `CREATE SCHEMA IF NOT EXISTS ` + quoteIdent(o.schema)
}

func (o options) createTableStatements() []string {
	return []string{
		// value is TEXT, not JSONB: nothing here reads inside the document,
		// and a parsing column would reorder keys and drop whitespace, so the
		// bytes Get returns would stop being the bytes Set was given.
		//
		// UNLOGGED sits before TABLE: CREATE [UNLOGGED] TABLE [IF NOT EXISTS].
		fmt.Sprintf(`CREATE%s TABLE IF NOT EXISTS %s (
    namespace  VARCHAR(%d) NOT NULL,
    key        VARCHAR(%d) NOT NULL,
    value      TEXT        NOT NULL,
    expires    BIGINT      NULL,
    PRIMARY KEY (namespace, key)
)`, o.persistence.keyword(), o.relation(), maxNamespaceLen, maxKeyLen),

		// Partial, so a sweep scans only what can expire.
		`CREATE INDEX IF NOT EXISTS ` + quoteIdent(o.table+"_expires_idx") +
			` ON ` + o.relation() + ` (expires) WHERE expires IS NOT NULL`,
	}
}

// EnsureSchema creates the schema, table and index when they do not exist
// yet, then verifies that the table's persistence is the one the store
// declares. It is idempotent, so every replica may call it at boot.
//
// A table that already exists is never converted: CREATE ... IF NOT EXISTS
// ignores the persistence clause, so a store whose declaration drifted from
// its table gets a *PersistenceMismatchError rather than silence. The check is
// the last step, so a caller that would rather log the drift than fail on it —
// errors.As picks it out — still holds a store whose relations all exist.
//
// It is a method rather than a free function so that the schema this store
// reads is the schema it created, by construction.
func (p *Postgres) EnsureSchema(ctx context.Context) error {
	statements := append([]string{p.opts.createSchemaStatement()}, p.opts.createTableStatements()...)

	// One statement per Exec: the extended protocol takes no multi-statement
	// strings.
	for _, statement := range statements {
		if _, err := p.db.ExecContext(ctx, statement); err != nil && !isDuplicateObject(err) {
			return fmt.Errorf("kvstore ensure schema: %w", err)
		}
	}
	return p.verifyPersistence(ctx)
}

// persistenceQuery reads the catalog by identifier value rather than by a
// quoted reference. The two are equal because validateIdent accepts only the
// already-folded lower-case form.
const persistenceQuery = `SELECT c.relpersistence::text
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2`

// verifyPersistence casts relpersistence to text because it is the internal
// "char" type, which no driver has to map to a Go string on its own.
func (p *Postgres) verifyPersistence(ctx context.Context) error {
	var relpersistence string
	err := p.db.QueryRowContext(ctx, persistenceQuery, p.opts.schema, p.opts.table).Scan(&relpersistence)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("kvstore verify persistence: %s does not exist", p.opts.relation())
	}
	if err != nil {
		return fmt.Errorf("kvstore verify persistence: %w", err)
	}

	got, ok := persistenceOf(relpersistence)
	if !ok {
		return fmt.Errorf("kvstore verify persistence: %s has relpersistence %q, which is neither logged nor unlogged",
			p.opts.relation(), relpersistence)
	}
	if got != p.opts.persistence {
		return &PersistenceMismatchError{Relation: p.opts.relation(), Want: p.opts.persistence, Got: got}
	}
	return nil
}

// PersistenceMismatchError reports a table whose durability is not the one the
// store declares. Both ways out are in the message, because choosing between
// them needs a maintenance window and a table size, which the library does
// not know.
type PersistenceMismatchError struct {
	Relation string
	Want     Persistence
	Got      Persistence
}

func (e *PersistenceMismatchError) Error() string {
	alter := "SET LOGGED"
	if e.Want == Unlogged {
		alter = "SET UNLOGGED"
	}
	return fmt.Sprintf(
		"kvstore: %s is %s but the store declares %s; EnsureSchema does not convert an existing table — "+
			"kvstore.RecreateTable is cheap and discards every entry, while ALTER TABLE %s %s keeps them "+
			"at the cost of a full table rewrite under ACCESS EXCLUSIVE",
		e.Relation, e.Got, e.Want, e.Relation, alter)
}

// RecreateTable drops the table and creates it again with the persistence the
// options declare, which discards every entry it held. Losing them is what
// makes it cheaper than ALTER TABLE ... SET LOGGED/UNLOGGED: neither a rewrite
// nor the old content through the WAL. It is the conversion for a cache, and
// an unlogged table is one by necessity — Postgres truncates it after a crash
// on its own.
//
// The drop and the create are separate statements on the handle given. Pass a
// *sql.Tx to make them one: DDL is transactional in Postgres, so a concurrent
// reader then waits on the lock rather than finding no table at all.
//
// It is a free function, like DropSchema, because destruction stays
// deliberate and out of reach of a running store.
func RecreateTable(ctx context.Context, db DBTX, opts ...Option) error {
	resolved, err := resolve(opts)
	if err != nil {
		return err
	}

	// No CASCADE: a view or a foreign key on this table is not something a
	// call here may take with it.
	statements := append([]string{
		resolved.createSchemaStatement(),
		`DROP TABLE IF EXISTS ` + resolved.relation(),
	}, resolved.createTableStatements()...)

	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil && !isDuplicateObject(err) {
			return fmt.Errorf("kvstore recreate table: %w", err)
		}
	}
	return nil
}

// DropSchema removes everything EnsureSchema created, the schema included.
// It is for test resets and local teardown, not for a running application,
// which is why it is not reachable from a store value.
func DropSchema(ctx context.Context, db DBTX, opts ...Option) error {
	resolved, err := resolve(opts)
	if err != nil {
		return err
	}

	// The drop is CASCADE, so a call here must not be able to take the host
	// application's own tables with it.
	if resolved.schema == "public" {
		return fmt.Errorf("kvstore: refusing to drop the %q schema", resolved.schema)
	}

	if _, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS `+quoteIdent(resolved.schema)+` CASCADE`); err != nil {
		return fmt.Errorf("kvstore drop schema: %w", err)
	}
	return nil
}

// sqlStater is asserted structurally so this package keeps no driver
// dependency; pgx's *pgconn.PgError satisfies it.
type sqlStater interface {
	error
	SQLState() string
}

// isDuplicateObject reports whether a concurrent first boot lost the CREATE
// race. IF NOT EXISTS does not prevent it: Postgres checks existence before
// taking the catalog lock, so the loser sees a duplicate-object error — and
// by then the object the caller asked for exists.
func isDuplicateObject(err error) bool {
	pgErr, ok := errors.AsType[sqlStater](err)
	if !ok {
		return false
	}
	// 42P06 schema, 42P07 table, 42710 index, 23505 the catalog unique index.
	return slices.Contains([]string{"23505", "42P06", "42P07", "42710"}, pgErr.SQLState())
}
