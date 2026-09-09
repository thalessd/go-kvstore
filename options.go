package kvstore

import "fmt"

const (
	// DefaultSchema keeps this package's table out of the host application's
	// schema, so its DDL and the application's migrations never collide.
	DefaultSchema = "kvstore"

	// DefaultTable is unambiguous inside a schema of this package's own.
	DefaultTable = "entries"
)

// Persistence is the table's durability. EnsureSchema asserts it against the
// table that exists rather than only applying it at creation: CREATE ... IF
// NOT EXISTS silently ignores the clause on a table that is already there, so
// a declaration that drifted from its table would never be heard.
type Persistence uint8

const (
	// Logged is the zero value, and Postgres's own default.
	Logged Persistence = iota

	// Unlogged trades crash-safety and replication for write speed: the table
	// is truncated after a crash, is invisible on a standby and is never
	// replicated. Only sensible when the table is a pure cache.
	Unlogged
)

func (p Persistence) String() string {
	switch p {
	case Logged:
		return "logged"
	case Unlogged:
		return "unlogged"
	default:
		return fmt.Sprintf("Persistence(%d)", uint8(p))
	}
}

// keyword sits before TABLE: CREATE [UNLOGGED] TABLE [IF NOT EXISTS].
func (p Persistence) keyword() string {
	if p == Unlogged {
		return " UNLOGGED"
	}
	return ""
}

// relpersistence is how pg_class spells the same property.
func (p Persistence) relpersistence() string {
	if p == Unlogged {
		return "u"
	}
	return "p"
}

// persistenceOf is the inverse, for reporting what the catalog holds.
func persistenceOf(relpersistence string) (Persistence, bool) {
	switch relpersistence {
	case "p":
		return Logged, true
	case "u":
		return Unlogged, true
	default:
		return 0, false
	}
}

type options struct {
	schema      string
	table       string
	persistence Persistence
}

// Option configures the Postgres store's physical layout.
type Option func(*options)

// WithSchema overrides the schema the store reads and writes.
func WithSchema(name string) Option { return func(o *options) { o.schema = name } }

// WithTable matters when the store shares a schema with something else; in a
// schema of its own the default name needs no override.
func WithTable(name string) Option { return func(o *options) { o.table = name } }

// WithPersistence declares the durability the table is expected to have.
// EnsureSchema creates the table that way and then verifies it, so this is an
// assertion about the deployed table and not only a creation-time flag.
// Changing it does not convert an existing table: see RecreateTable.
func WithPersistence(p Persistence) Option { return func(o *options) { o.persistence = p } }

// Deprecated: use WithPersistence(Unlogged).
func WithUnlogged() Option { return WithPersistence(Unlogged) }

func resolve(opts []Option) (options, error) {
	resolved := options{schema: DefaultSchema, table: DefaultTable}
	for _, opt := range opts {
		opt(&resolved)
	}
	if err := validateIdent("schema", resolved.schema); err != nil {
		return options{}, err
	}
	if err := validateIdent("table", resolved.table); err != nil {
		return options{}, err
	}
	if resolved.persistence != Logged && resolved.persistence != Unlogged {
		return options{}, fmt.Errorf(
			"kvstore: unknown persistence %s; use kvstore.Logged or kvstore.Unlogged", resolved.persistence)
	}
	return resolved, nil
}

// relation is the fully qualified, quoted name every statement uses, so the
// store ignores the connection's search_path.
func (o options) relation() string {
	return quoteIdent(o.schema) + "." + quoteIdent(o.table)
}
