// SPDX-License-Identifier: Apache-2.0

package sql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/sinktest"
	sqlsink "github.com/stablekernel/crucible/sink/sql"
)

// fakeTx is a hand-rolled Tx implementation — no driver, no mockery.
type fakeTx struct {
	calls []execCall
	err   error
}

type execCall struct {
	query string
	args  []any
}

func (f *fakeTx) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.calls = append(f.calls, execCall{query: query, args: args})
	if f.err != nil {
		return nil, f.err
	}
	return driverResult{}, nil
}

type driverResult struct{}

func (driverResult) LastInsertId() (int64, error) { return 0, nil }
func (driverResult) RowsAffected() (int64, error) { return 1, nil }

type orderPlaced struct{ ID string }

func newOutlet(tx sqlsink.Tx) csink.Outlet {
	reg := sqlsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[sqlsink.Tx] {
		return sqlsink.Exec("INSERT INTO orders(id) VALUES (?)", o.ID)
	})
	return sqlsink.New(tx, reg)
}

func TestExecRunsStatement(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{}
	if err := newOutlet(tx).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(tx.calls) != 1 || tx.calls[0].query != "INSERT INTO orders(id) VALUES (?)" {
		t.Fatalf("ExecContext calls = %+v, want one INSERT", tx.calls)
	}
	if len(tx.calls[0].args) != 1 || tx.calls[0].args[0] != "A-1" {
		t.Fatalf("args = %v, want [A-1]", tx.calls[0].args)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := newOutlet(&fakeTx{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestExecErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("constraint violation")
	err := newOutlet(&fakeTx{err: boom}).Sink(context.Background(), orderPlaced{ID: "A-2"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "sql" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:sql, Phase:apply}", se)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeTx{}) })
}
