// SPDX-License-Identifier: Apache-2.0

//go:build integration

package sql_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	csink "github.com/stablekernel/crucible/sink"
	sqlsink "github.com/stablekernel/crucible/sink/sql"
)

// userRegisteredIT is the payload the integration test sinks through the outlet.
type userRegisteredIT struct {
	Email string
}

// TestIntegrationSinkLandsRowInRealSQLite drives the real Outlet path against an
// in-memory SQLite database opened through the pure-Go modernc.org/sqlite
// driver, then reads the row back to prove the write landed.
func TestIntegrationSinkLandsRowInRealSQLite(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("db.Close() error = %v", cerr)
		}
	})

	ctx := context.Background()
	if _, err = db.ExecContext(ctx, `CREATE TABLE users (email TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}

	reg := sqlsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, u userRegisteredIT) csink.Op[sqlsink.Tx] {
		return sqlsink.Exec("INSERT INTO users(email) VALUES (?)", u.Email)
	})

	outlet := sqlsink.New(db, reg)
	if err = outlet.Sink(ctx, userRegisteredIT{Email: "ada@example.com"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	var got string
	if err = db.QueryRowContext(ctx, `SELECT email FROM users`).Scan(&got); err != nil {
		t.Fatalf("scan inserted row error = %v", err)
	}
	if got != "ada@example.com" {
		t.Fatalf("persisted email = %q, want %q", got, "ada@example.com")
	}
}
