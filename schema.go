package kvstore

import (
	"context"
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

// EnsureSchema creates the schema, table and index when they do not exist
// yet. It is idempotent, so every replica may call it at boot.
//
// It is a method rather than a free function so that the schema this store
// reads is the schema it created, by construction.
func (p *Postgres) EnsureSchema(ctx context.Context) error {
	unlogged := ""
	if p.opts.unlogged {
		unlogged = " UNLOGGED"
	}

	// UNLOGGED sits before TABLE: CREATE [UNLOGGED] TABLE [IF NOT EXISTS].
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + quoteIdent(p.opts.schema),

		// value is TEXT, not JSONB: nothing here reads inside the document,
		// and a parsing column would reorder keys and drop whitespace, so the
		// bytes Get returns would stop being the bytes Set was given.
		fmt.Sprintf(`CREATE%s TABLE IF NOT EXISTS %s (
    namespace  VARCHAR(%d) NOT NULL,
    key        VARCHAR(%d) NOT NULL,
    value      TEXT        NOT NULL,
    expires    BIGINT      NULL,
    PRIMARY KEY (namespace, key)
)`, unlogged, p.opts.relation(), maxNamespaceLen, maxKeyLen),

		// Partial, so a sweep scans only what can expire.
		`CREATE INDEX IF NOT EXISTS ` + quoteIdent(p.opts.table+"_expires_idx") +
			` ON ` + p.opts.relation() + ` (expires) WHERE expires IS NOT NULL`,
	}

	// One statement per Exec: the extended protocol takes no multi-statement
	// strings.
	for _, statement := range statements {
		if _, err := p.db.ExecContext(ctx, statement); err != nil && !isDuplicateObject(err) {
			return fmt.Errorf("kvstore ensure schema: %w", err)
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
