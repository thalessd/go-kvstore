package kvstore

import "fmt"

// maxIdentLen is NAMEDATALEN-1. Postgres truncates a longer name server-side
// without complaining, which would leave the DDL and the DML disagreeing
// about which relation they mean.
const maxIdentLen = 63

// validateIdent is deliberately stricter than Postgres: lower-case ASCII,
// digits and underscore, never leading with a digit. Nothing that survives it
// can carry a quote, a dot or a byte whose meaning changes once quoted, which
// is what lets the identifier be interpolated into a statement at all.
//
// Unquoted identifiers fold to lower case in Postgres, so accepting only the
// folded form keeps a quoted reference and a hand-written unquoted one equal.
func validateIdent(kind, name string) error {
	if name == "" {
		return fmt.Errorf("kvstore: %s must not be empty", kind)
	}
	if len(name) > maxIdentLen {
		return fmt.Errorf("kvstore: %s %q is longer than %d bytes", kind, name, maxIdentLen)
	}
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return fmt.Errorf(
				"kvstore: %s %q has an invalid character %q at %d; use lower-case letters, digits and underscore, not starting with a digit",
				kind, name, rune(c), i)
		}
	}
	return nil
}

// quoteIdent wraps a validated identifier so a reserved word works as a name
// and search_path cannot redirect the reference.
func quoteIdent(name string) string { return `"` + name + `"` }
