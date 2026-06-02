// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"context"
	"database/sql"
	"fmt"

	csink "github.com/stablekernel/crucible/sink"
	sqlsink "github.com/stablekernel/crucible/sink/sql"
)

// recordingTx is a stand-in Tx that records the statements it runs.
type recordingTx struct{ stmts []string }

func (r *recordingTx) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	r.stmts = append(r.stmts, query)
	return nil, nil
}

type userRegistered struct{ Email string }

func ExampleNew() {
	tx := &recordingTx{}
	reg := sqlsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, u userRegistered) csink.Op[sqlsink.Tx] {
		return sqlsink.Exec("INSERT INTO users(email) VALUES (?)", u.Email)
	})

	outlet := sqlsink.New(tx, reg)
	_ = outlet.Sink(context.Background(), userRegistered{Email: "a@example.com"})

	fmt.Println(tx.stmts[0])
	// Output: INSERT INTO users(email) VALUES (?)
}
