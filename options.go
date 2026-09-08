package kvstore

const (
	// DefaultSchema keeps this package's table out of the host application's
	// schema, so its DDL and the application's migrations never collide.
	DefaultSchema = "kvstore"

	// DefaultTable is unambiguous inside a schema of this package's own.
	DefaultTable = "entries"
)

type options struct {
	schema   string
	table    string
	unlogged bool
}

// Option configures the Postgres store's physical layout.
type Option func(*options)

// WithSchema overrides the schema the store reads and writes.
func WithSchema(name string) Option { return func(o *options) { o.schema = name } }

// WithTable matters when the store shares a schema with something else; in a
// schema of its own the default name needs no override.
func WithTable(name string) Option { return func(o *options) { o.table = name } }

// WithUnlogged trades crash-safety and replication for write speed, which is
// only sensible when the table is a pure cache. Read by EnsureSchema; the
// data path ignores it.
func WithUnlogged() Option { return func(o *options) { o.unlogged = true } }

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
	return resolved, nil
}

// relation is the fully qualified, quoted name every statement uses, so the
// store ignores the connection's search_path.
func (o options) relation() string {
	return quoteIdent(o.schema) + "." + quoteIdent(o.table)
}
