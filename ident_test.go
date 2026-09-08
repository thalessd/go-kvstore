package kvstore

import (
	"strings"
	"testing"
)

func TestValidateIdent(t *testing.T) {
	tests := []struct {
		name  string
		ident string
		valid bool
	}{
		{"plain", "kv", true},
		{"with digit", "kv_1", true},
		{"leading underscore", "_kv", true},
		{"max length", strings.Repeat("a", maxIdentLen), true},
		{"empty", "", false},
		{"upper case", "KV", false},
		{"leading digit", "1kv", false},
		{"hyphen", "kv-store", false},
		{"space", "kv store", false},
		{"statement terminator", "kv;drop", false},
		{"embedded quote", `kv"x`, false},
		{"qualified", "public.kv", false},
		{"too long", strings.Repeat("a", maxIdentLen+1), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateIdent("schema", test.ident)
			if test.valid && err != nil {
				t.Errorf("validateIdent(%q) = %v, want nil", test.ident, err)
			}
			if !test.valid && err == nil {
				t.Errorf("validateIdent(%q) = nil, want an error", test.ident)
			}
		})
	}
}

func TestResolveDefaultsToItsOwnSchema(t *testing.T) {
	resolved, err := resolve(nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.schema != DefaultSchema || resolved.table != DefaultTable {
		t.Errorf("resolve() = %s.%s, want %s.%s",
			resolved.schema, resolved.table, DefaultSchema, DefaultTable)
	}
	if got, want := resolved.relation(), `"kvstore"."entries"`; got != want {
		t.Errorf("relation() = %s, want %s", got, want)
	}
}
