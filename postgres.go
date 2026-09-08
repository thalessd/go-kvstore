package kvstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DBTX is everything the store needs from a database handle. *sql.DB and
// *sql.Tx both satisfy it, so the store composes inside a transaction the
// caller already owns.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// statements are built once per store, so the only interpolated text is a
// relation name that validateIdent already accepted. "now" is always a
// Go-computed parameter rather than the server's clock.
type statements struct {
	get    string
	upsert string
	del    string
	has    string
	clear  string
	purge  string
}

func buildStatements(relation string) statements {
	return statements{
		get: `SELECT value FROM ` + relation + `
WHERE namespace = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > $3::timestamptz)`,

		upsert: `INSERT INTO ` + relation + ` (namespace, key, value, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (namespace, key) DO UPDATE
SET value = excluded.value, expires_at = excluded.expires_at, updated_at = now()`,

		del: `DELETE FROM ` + relation + ` WHERE namespace = $1 AND key = ANY($2)`,

		has: `SELECT EXISTS (
	SELECT 1 FROM ` + relation + `
	WHERE namespace = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > $3::timestamptz))`,

		clear: `DELETE FROM ` + relation + ` WHERE namespace = $1`,

		purge: `DELETE FROM ` + relation + ` WHERE expires_at <= $1::timestamptz`,
	}
}

// Postgres stores entries in one table on the caller's pool. Every statement
// names the relation fully qualified, so the store shares a pool with the
// host application without depending on its search_path.
//
// Delete passes a []string to key = ANY($2), which requires a driver that
// encodes a Go slice as a Postgres array; pgx/v5/stdlib does.
type Postgres struct {
	db   DBTX
	opts options
	stmt statements
}

// NewPostgres returns a store on the given handle. It fails on an
// unusable schema or table name rather than falling back to a default,
// because a typo would otherwise send every write to the wrong relation.
func NewPostgres(db DBTX, opts ...Option) (*Postgres, error) {
	resolved, err := resolve(opts)
	if err != nil {
		return nil, err
	}
	return &Postgres{
		db:   db,
		opts: resolved,
		stmt: buildStatements(resolved.relation()),
	}, nil
}

// WithTx returns a store bound to a transaction, for callers composing a kv
// write with other statements in one transaction. The clone carries the
// layout, so the copy reads the relation the original was configured with.
func (p *Postgres) WithTx(tx *sql.Tx) *Postgres {
	clone := *p
	clone.db = tx
	return &clone
}

func (p *Postgres) Get(ctx context.Context, namespace, key string) (json.RawMessage, bool, error) {
	var value json.RawMessage
	err := p.db.QueryRowContext(ctx, p.stmt.get, namespace, key, time.Now().UTC()).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("kvstore get %s/%s: %w", namespace, key, err)
	}
	return value, true, nil
}

func (p *Postgres) Set(ctx context.Context, namespace, key string, value json.RawMessage, expires time.Time) error {
	expiresAt := sql.NullTime{Time: expires.UTC(), Valid: !expires.IsZero()}
	if _, err := p.db.ExecContext(ctx, p.stmt.upsert, namespace, key, []byte(value), expiresAt); err != nil {
		return fmt.Errorf("kvstore set %s/%s: %w", namespace, key, err)
	}
	return nil
}

func (p *Postgres) Delete(ctx context.Context, namespace string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if _, err := p.db.ExecContext(ctx, p.stmt.del, namespace, keys); err != nil {
		return fmt.Errorf("kvstore delete %s: %w", namespace, err)
	}
	return nil
}

func (p *Postgres) Has(ctx context.Context, namespace, key string) (bool, error) {
	var exists bool
	err := p.db.QueryRowContext(ctx, p.stmt.has, namespace, key, time.Now().UTC()).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("kvstore has %s/%s: %w", namespace, key, err)
	}
	return exists, nil
}

func (p *Postgres) Clear(ctx context.Context, namespace string) error {
	if _, err := p.db.ExecContext(ctx, p.stmt.clear, namespace); err != nil {
		return fmt.Errorf("kvstore clear %s: %w", namespace, err)
	}
	return nil
}

// PurgeExpired deletes every already-expired row and reports how many. Reads
// filter expires_at themselves, so nothing depends on this being prompt; a
// cleanup ticker calls it only to keep the table from growing without bound.
func (p *Postgres) PurgeExpired(ctx context.Context, now time.Time) (int64, error) {
	res, err := p.db.ExecContext(ctx, p.stmt.purge, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("kvstore purge expired: %w", err)
	}
	return res.RowsAffected()
}
