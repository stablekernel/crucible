// SPDX-License-Identifier: Apache-2.0

// Package slog is a sink destination that emits payloads as structured log
// records via the standard library's log/slog. It depends only on the standard
// library and crucible/sink. Register a transformer that turns each payload
// type into a [Log] operation, then attach the result of [New] to a
// sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package slog

import (
	"context"
	"log/slog"

	csink "github.com/stablekernel/crucible/sink"
)

// Log returns an Op that emits a single structured log record at the given
// level. Each attr is attached to the record verbatim. It is the core
// constructor: a registry maps each payload type to the Log (or OpFunc) that
// records it.
func Log(level slog.Level, msg string, attrs ...slog.Attr) csink.Op[*slog.Logger] {
	return csink.OpFunc[*slog.Logger](func(ctx context.Context, logger *slog.Logger) error {
		logger.LogAttrs(ctx, level, msg, attrs...)
		return nil
	})
}

// Info returns an Op that emits a record at [slog.LevelInfo].
func Info(msg string, attrs ...slog.Attr) csink.Op[*slog.Logger] {
	return Log(slog.LevelInfo, msg, attrs...)
}

// Error returns an Op that emits a record at [slog.LevelError].
func Error(msg string, attrs ...slog.Attr) csink.Op[*slog.Logger] {
	return Log(slog.LevelError, msg, attrs...)
}

// NewRegistry returns an empty registry of Op[*slog.Logger] for callers to
// populate with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[*slog.Logger]] {
	return csink.NewRegistry[csink.Op[*slog.Logger]]()
}

// New builds an Outlet that applies each payload's registered Op[*slog.Logger]
// to logger. The outlet is named "slog" unless overridden with sink.WithName.
func New(logger *slog.Logger, reg *csink.Registry[csink.Op[*slog.Logger]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[*slog.Logger](logger, reg, append([]csink.EmitterOption{csink.WithName("slog")}, opts...)...)
}
