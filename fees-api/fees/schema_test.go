package fees

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestLedgerSchemaTablesAndColumns(t *testing.T) {
	ctx := context.Background()

	assertTableExists(t, ctx, "bills")
	assertTableExists(t, ctx, "line_items")
	assertTableExists(t, ctx, "currencies")

	bills := loadColumns(t, ctx, "bills")
	assertColumn(t, bills, "bill_id", columnExpectation{dataType: "text", nullable: false})
	assertColumn(t, bills, "client_id", columnExpectation{dataType: "text", nullable: false})
	assertColumn(t, bills, "currency", columnExpectation{dataType: "character", charLength: 3, nullable: false})
	assertColumn(t, bills, "period", columnExpectation{dataType: "text", nullable: false})
	assertColumn(t, bills, "status", columnExpectation{dataType: "text", nullable: false, defaultContains: "'OPEN'::text"})
	assertColumn(t, bills, "opened_at", columnExpectation{dataType: "timestamp with time zone", nullable: false, defaultContains: "now()"})
	assertColumn(t, bills, "closed_at", columnExpectation{dataType: "timestamp with time zone", nullable: true})

	lineItems := loadColumns(t, ctx, "line_items")
	assertColumn(t, lineItems, "id", columnExpectation{dataType: "bigint", nullable: false, identity: true})
	assertColumn(t, lineItems, "bill_id", columnExpectation{dataType: "text", nullable: false})
	assertColumn(t, lineItems, "reference", columnExpectation{dataType: "text", nullable: false})
	assertColumn(t, lineItems, "amount_minor", columnExpectation{dataType: "bigint", nullable: false})
	assertColumn(t, lineItems, "currency", columnExpectation{dataType: "character", charLength: 3, nullable: false})
	assertColumn(t, lineItems, "fee_type", columnExpectation{dataType: "text", nullable: false})
	assertColumn(t, lineItems, "description", columnExpectation{dataType: "text", nullable: false, defaultContains: "''::text"})
	assertColumn(t, lineItems, "status", columnExpectation{dataType: "text", nullable: false})
	assertColumnHasNoDefault(t, lineItems, "status")
	assertColumn(t, lineItems, "applied_at", columnExpectation{dataType: "timestamp with time zone", nullable: false, defaultContains: "now()"})

	currencies := loadColumns(t, ctx, "currencies")
	assertColumn(t, currencies, "code", columnExpectation{dataType: "character", charLength: 3, nullable: false})
	assertColumn(t, currencies, "exponent", columnExpectation{dataType: "smallint", nullable: false})
	assertColumn(t, currencies, "display_name", columnExpectation{dataType: "text", nullable: false, defaultContains: "''::text"})
}

func TestLedgerSchemaConstraintsAndIndexes(t *testing.T) {
	ctx := context.Background()

	bills := loadConstraints(t, ctx, "bills")
	assertConstraint(t, bills, "p", "PRIMARY KEY (bill_id)")
	assertConstraint(t, bills, "u", "UNIQUE (client_id, currency, period)")
	assertConstraint(t, bills, "c", "status", "OPEN", "CLOSED")

	lineItems := loadConstraints(t, ctx, "line_items")
	assertConstraint(t, lineItems, "p", "PRIMARY KEY (id)")
	assertConstraint(t, lineItems, "u", "UNIQUE (bill_id, reference)")
	assertConstraint(t, lineItems, "f", "FOREIGN KEY (bill_id)", "REFERENCES bills(bill_id)")
	assertConstraint(t, lineItems, "c", "status", "PENDING", "FINALIZED", "FAILED")

	currencies := loadConstraints(t, ctx, "currencies")
	assertConstraint(t, currencies, "p", "PRIMARY KEY (code)")

	assertIndexExists(t, ctx, "idx_bills_client_status")
	assertIndexExists(t, ctx, "idx_bills_period")
	assertIndexExists(t, ctx, "idx_bills_currency")
	assertIndexExists(t, ctx, "idx_line_items_bill")
}

func TestCurrenciesSeedRows(t *testing.T) {
	ctx := context.Background()

	rows, err := db.Query(ctx, `
		SELECT code, exponent
		  FROM currencies
		 ORDER BY code`)
	if err != nil {
		t.Fatalf("query currencies: %v", err)
	}
	defer rows.Close()

	got := make(map[string]int)
	for rows.Next() {
		var code string
		var exponent int
		if err := rows.Scan(&code, &exponent); err != nil {
			t.Fatalf("scan currency row: %v", err)
		}
		got[strings.TrimSpace(code)] = exponent
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate currencies: %v", err)
	}

	want := map[string]int{"GEL": 2, "USD": 2}
	if len(got) != len(want) {
		t.Fatalf("seeded currencies = %#v, want exactly %#v", got, want)
	}
	for code, exponent := range want {
		if got[code] != exponent {
			t.Fatalf("currency %s exponent = %d, want %d; all rows: %#v", code, got[code], exponent, got)
		}
	}
}

func TestSupportedCurrencyLookupUsesCurrenciesTable(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		code string
		want bool
	}{
		{code: "GEL", want: true},
		{code: "USD", want: true},
		{code: "EUR", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			got, err := isSupportedCurrency(ctx, tt.code)
			if err != nil {
				t.Fatalf("isSupportedCurrency(%q): %v", tt.code, err)
			}
			if got != tt.want {
				t.Fatalf("isSupportedCurrency(%q) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestLedgerConstraintsRejectBadInserts(t *testing.T) {
	ctx := context.Background()
	// Bad status is rejected by the CHECK.
	_, err := db.Exec(ctx, `
		INSERT INTO bills (bill_id, client_id, currency, period, status)
		VALUES ('bill-schema-guard-bad-status-USD-2099-01', 'schema-guard', 'USD', '2099-01', 'DRAINING')`)
	assertPgErrorCode(t, err, "23514")

	// Orphan line_item is rejected by the FK.
	_, err = db.Exec(ctx, `
		INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type, status)
		VALUES ('bill-schema-guard-missing-USD-2099-01', 'schema-guard-orphan', 100, 'USD', 'wire_transfer', 'FINALIZED')`)
	assertPgErrorCode(t, err, "23503")

	// Setup one bill so the two uniqueness checks have a valid target.
	if _, err := db.Exec(ctx, `
		INSERT INTO bills (bill_id, client_id, currency, period)
		VALUES ('bill-schema-guard-USD-2099-02', 'schema-guard', 'USD', '2099-02')`); err != nil {
		t.Fatalf("seed bill for uniqueness checks: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(ctx, `DELETE FROM line_items WHERE bill_id = 'bill-schema-guard-USD-2099-02'`)
		_, _ = db.Exec(ctx, `DELETE FROM bills WHERE bill_id = 'bill-schema-guard-USD-2099-02'`)
	})

	// Duplicate (client_id, currency, period).
	_, err = db.Exec(ctx, `
		INSERT INTO bills (bill_id, client_id, currency, period)
		VALUES ('bill-schema-guard-USD-2099-02-dup', 'schema-guard', 'USD', '2099-02')`)
	assertPgErrorCode(t, err, "23505")

	// Every supported line-item status is accepted.
	for _, status := range []string{"PENDING", "FINALIZED", "FAILED"} {
		if _, err := db.Exec(ctx, `
			INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type, status)
			VALUES ('bill-schema-guard-USD-2099-02', $1, 100, 'USD', 'wire_transfer', $2)`,
			"schema-guard-status-"+strings.ToLower(status),
			status,
		); err != nil {
			t.Fatalf("insert line item with status %s: %v", status, err)
		}
	}

	// Status is required and has no implicit default.
	_, err = db.Exec(ctx, `
		INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type)
		VALUES ('bill-schema-guard-USD-2099-02', 'schema-guard-status-omitted', 100, 'USD', 'wire_transfer')`)
	assertPgErrorCode(t, err, "23502")

	_, err = db.Exec(ctx, `
		INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type, status)
		VALUES ('bill-schema-guard-USD-2099-02', 'schema-guard-status-null', 100, 'USD', 'wire_transfer', NULL)`)
	assertPgErrorCode(t, err, "23502")

	_, err = db.Exec(ctx, `
		INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type, status)
		VALUES ('bill-schema-guard-USD-2099-02', 'schema-guard-status-invalid', 100, 'USD', 'wire_transfer', 'UNKNOWN')`)
	assertPgErrorCode(t, err, "23514")

	// Duplicate (bill_id, reference).
	if _, err := db.Exec(ctx, `
		INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type, status)
		VALUES ('bill-schema-guard-USD-2099-02', 'schema-guard-ref-dup', 100, 'USD', 'wire_transfer', 'FINALIZED')`); err != nil {
		t.Fatalf("seed line item for uniqueness check: %v", err)
	}

	_, err = db.Exec(ctx, `
		INSERT INTO line_items (bill_id, reference, amount_minor, currency, fee_type, status)
		VALUES ('bill-schema-guard-USD-2099-02', 'schema-guard-ref-dup', 200, 'USD', 'wire_transfer', 'FINALIZED')`)
	assertPgErrorCode(t, err, "23505")
}

type columnInfo struct {
	dataType string
	nullable bool
	charLen  int
	def      string
	identity bool
}

type columnExpectation struct {
	dataType        string
	nullable        bool
	charLength      int
	defaultContains string
	identity        bool
}

type constraintInfo struct {
	kind       string
	definition string
}

func assertTableExists(t *testing.T, ctx context.Context, table string) {
	t.Helper()

	var exists bool
	if err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM information_schema.tables
			 WHERE table_schema = 'public'
			   AND table_name = $1
		)`, table).Scan(&exists); err != nil {
		t.Fatalf("check table %s exists: %v", table, err)
	}
	if !exists {
		t.Fatalf("expected table %s to exist", table)
	}
}

func loadColumns(t *testing.T, ctx context.Context, table string) map[string]columnInfo {
	t.Helper()

	rows, err := db.Query(ctx, `
		SELECT column_name,
		       data_type,
		       is_nullable,
		       character_maximum_length,
		       column_default,
		       is_identity
		  FROM information_schema.columns
		 WHERE table_schema = 'public'
		   AND table_name = $1`, table)
	if err != nil {
		t.Fatalf("query columns for %s: %v", table, err)
	}
	defer rows.Close()

	out := make(map[string]columnInfo)
	for rows.Next() {
		var name string
		var maxLen sql.NullInt64
		var def sql.NullString
		var nullable string
		var identity string
		var col columnInfo
		if err := rows.Scan(&name, &col.dataType, &nullable, &maxLen, &def, &identity); err != nil {
			t.Fatalf("scan column for %s: %v", table, err)
		}
		col.nullable = nullable == "YES"
		col.identity = identity == "YES"
		if maxLen.Valid {
			col.charLen = int(maxLen.Int64)
		}
		if def.Valid {
			col.def = def.String
		}
		out[name] = col
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate columns for %s: %v", table, err)
	}
	return out
}

func assertColumn(t *testing.T, columns map[string]columnInfo, name string, want columnExpectation) {
	t.Helper()

	got, ok := columns[name]
	if !ok {
		t.Fatalf("missing column %s", name)
	}
	if got.dataType != want.dataType {
		t.Fatalf("%s data type = %q, want %q", name, got.dataType, want.dataType)
	}
	if got.nullable != want.nullable {
		t.Fatalf("%s nullable = %v, want %v", name, got.nullable, want.nullable)
	}
	if want.charLength != 0 && got.charLen != want.charLength {
		t.Fatalf("%s char length = %d, want %d", name, got.charLen, want.charLength)
	}
	if want.defaultContains != "" && !strings.Contains(got.def, want.defaultContains) {
		t.Fatalf("%s default = %q, want to contain %q", name, got.def, want.defaultContains)
	}
	if got.identity != want.identity {
		t.Fatalf("%s identity = %v, want %v", name, got.identity, want.identity)
	}
}

func assertColumnHasNoDefault(t *testing.T, columns map[string]columnInfo, name string) {
	t.Helper()

	column, ok := columns[name]
	if !ok {
		t.Fatalf("missing column %s", name)
	}
	if column.def != "" {
		t.Fatalf("%s default = %q, want no default", name, column.def)
	}
}

func loadConstraints(t *testing.T, ctx context.Context, table string) []constraintInfo {
	t.Helper()

	rows, err := db.Query(ctx, `
		SELECT c.contype::text, pg_get_constraintdef(c.oid)
		  FROM pg_constraint c
		  JOIN pg_class tbl ON tbl.oid = c.conrelid
		  JOIN pg_namespace n ON n.oid = tbl.relnamespace
		 WHERE n.nspname = 'public'
		   AND tbl.relname = $1`, table)
	if err != nil {
		t.Fatalf("query constraints for %s: %v", table, err)
	}
	defer rows.Close()

	var out []constraintInfo
	for rows.Next() {
		var c constraintInfo
		if err := rows.Scan(&c.kind, &c.definition); err != nil {
			t.Fatalf("scan constraint for %s: %v", table, err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate constraints for %s: %v", table, err)
	}
	return out
}

func assertConstraint(t *testing.T, constraints []constraintInfo, kind string, fragments ...string) {
	t.Helper()

	for _, c := range constraints {
		if c.kind != kind {
			continue
		}
		matches := true
		for _, fragment := range fragments {
			if !strings.Contains(c.definition, fragment) {
				matches = false
				break
			}
		}
		if matches {
			return
		}
	}
	t.Fatalf("missing constraint kind %q containing %q; got %#v", kind, fragments, constraints)
}

func assertIndexExists(t *testing.T, ctx context.Context, name string) {
	t.Helper()

	var exists bool
	if err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM pg_indexes
			 WHERE schemaname = 'public'
			   AND indexname = $1
		)`, name).Scan(&exists); err != nil {
		t.Fatalf("check index %s exists: %v", name, err)
	}
	if !exists {
		t.Fatalf("expected index %s to exist", name)
	}
}

func assertPgErrorCode(t *testing.T, err error, code string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected PostgreSQL error code %s, got nil", code)
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected PostgreSQL error code %s, got non-PostgreSQL error %T: %v", code, err, err)
	}
	if pgErr.Code != code {
		t.Fatalf("PostgreSQL error code = %s (%s), want %s; error: %v", pgErr.Code, pgErr.Message, code, err)
	}
}
