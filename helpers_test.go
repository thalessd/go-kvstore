package kvstore

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return raw
}

func assertJSON(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	if string(got) != want {
		t.Errorf("value = %s, want %s", got, want)
	}
}

// newMockDB always installs stringArrayConverter, because sqlmock's option
// type is unexported and a variadic wrapper is therefore impossible. The
// converter is permissive, so the tests that do not need it are unaffected.
func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.ValueConverterOption(stringArrayConverter{}))
	if err != nil {
		t.Fatalf("open mock database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sql expectations: %v", err)
		}
	})
	return db, mock
}

// pgx encodes []string natively for ANY($n), but sqlmock's default converter
// rejects slices before an expectation ever sees them.
type stringArrayConverter struct{}

func (stringArrayConverter) ConvertValue(v any) (driver.Value, error) {
	if _, ok := v.([]string); ok {
		return v, nil
	}
	return driver.DefaultParameterConverter.ConvertValue(v)
}

// anyTime matches a timestamp the store computes itself.
func anyTime() sqlmock.Argument { return anyTimeArg{} }

type anyTimeArg struct{}

func (anyTimeArg) Match(driver.Value) bool { return true }
