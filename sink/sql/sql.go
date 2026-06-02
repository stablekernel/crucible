// SPDX-License-Identifier: Apache-2.0

// Package sql is a sink destination that persists payloads through the standard
// library's database/sql. It depends only on the standard library and
// crucible/sink — there is no driver or ORM dependency. Register a transformer
// that turns each payload type into an [Exec] operation, then attach the result
// of [New] to a sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package sql

import (
	"context"
	"database/sql"

	csink "github.com/stablekernel/crucible/sink"
)

// Tx is the narrow database/sql surface this destination needs. It is satisfied
// by *sql.DB, *sql.Tx, and *sql.Conn, so a consumer wires whichever scope fits
// without this package importing a driver.
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Exec returns an Op that runs a single statement with positional arguments. It
// is the workhorse constructor: a registry maps each payload type to the Exec
// (or OpFunc) that persists it.
func Exec(query string, args ...any) csink.Op[Tx] {
	return csink.OpFunc[Tx](func(ctx context.Context, tx Tx) error {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	})
}

// NewRegistry returns an empty registry of Op[Tx] for callers to populate with
// sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Tx]] {
	return csink.NewRegistry[csink.Op[Tx]]()
}

// New builds an Outlet that applies each payload's registered Op[Tx] to tx. The
// outlet is named "sql" unless overridden with sink.WithName.
func New(tx Tx, reg *csink.Registry[csink.Op[Tx]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Tx](tx, reg, append([]csink.EmitterOption{csink.WithName("sql")}, opts...)...)
}
