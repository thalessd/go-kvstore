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
		get: `SELECT value, expires FROM ` + relation + `
WHERE namespace = $1 AND key = $2 AND (expires IS NULL OR expires > $3)`,

		upsert: `INSERT INTO ` + relation + ` (namespace, key, value, expires)
VALUES ($1, $2, $3, $4)
ON CONFLICT (namespace, key) DO UPDATE
SET value = excluded.value, expires = excluded.expires`,

		del: `DELETE FROM ` + relation + ` WHERE namespace = $1 AND key = ANY($2)`,

		has: `SELECT EXISTS (
	SELECT 1 FROM ` + relation + `
	WHERE namespace = $1 AND key = $2 AND (expires IS NULL OR expires > $3))`,

		clear: `DELETE FROM ` + relation + ` WHERE namespace = $1`,

		purge: `DELETE FROM ` + relation + ` WHERE expires <= $1`,
	}
}

// epoch is how an expiry moment reaches the expires column: milliseconds since
// the Unix epoch, as keyv stores it. The truncation loses the sub-millisecond
// part, so an entry can expire up to a millisecond early — MemoryStore, which
// keeps the time.Time, is precise to the nanosecond.
//
// A zero moment means "never" and has to arrive as NULL: UnixMilli of a zero
// time.Time is a large negative number, which would read as long expired.
func epoch(moment time.Time) sql.NullInt64 {
	return sql.NullInt64{Int64: moment.UnixMilli(), Valid: !moment.IsZero()}
}

// momentOf is the inverse of epoch. UnixMilli returns a Time in the local
// zone, so the moment is normalized to UTC: Cache[T] hands Set a UTC moment,
// and a round trip through the column must not change the zone.
func momentOf(stored sql.NullInt64) time.Time {
	if !stored.Valid {
		return time.Time{}
	}
	return time.UnixMilli(stored.Int64).UTC()
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
	entry, found, err := p.GetEntry(ctx, namespace, key)
	return entry.Value, found, err
}

func (p *Postgres) GetEntry(ctx context.Context, namespace, key string) (Entry, bool, error) {
	// Scanned as a string because the column is TEXT: database/sql only
	// assigns a driver string to *[]byte itself, and json.RawMessage is a
	// named type its reflect path will not take.
	var value string
	var expires sql.NullInt64
	err := p.db.QueryRowContext(ctx, p.stmt.get, namespace, key, time.Now().UnixMilli()).Scan(&value, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("kvstore get %s/%s: %w", namespace, key, err)
	}
	return Entry{Value: json.RawMessage(value), Expires: momentOf(expires)}, true, nil
}

func (p *Postgres) Set(ctx context.Context, namespace, key string, value json.RawMessage, expires time.Time) error {
	if _, err := p.db.ExecContext(ctx, p.stmt.upsert, namespace, key, string(value), epoch(expires)); err != nil {
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
	err := p.db.QueryRowContext(ctx, p.stmt.has, namespace, key, time.Now().UnixMilli()).Scan(&exists)
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
// filter expires themselves, so nothing depends on this being prompt; a
// cleanup ticker calls it only to keep the table from growing without bound.
func (p *Postgres) PurgeExpired(ctx context.Context, now time.Time) (int64, error) {
	res, err := p.db.ExecContext(ctx, p.stmt.purge, now.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("kvstore purge expired: %w", err)
	}
	return res.RowsAffected()
}
