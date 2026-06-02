// SPDX-License-Identifier: Apache-2.0

// Package file is a sink destination that appends one JSON line per payload to
// an [io.Writer] or to a file opened by path. It depends only on the standard
// library and crucible/sink.
//
// Use [New] to wrap any io.Writer, or [Open] to have the outlet own an
// append-only file. Each call to Sink marshals the payload as JSON and writes
// it followed by a newline, producing a valid JSONL (newline-delimited JSON)
// stream. The outlet is safe for concurrent use.
//
// The outlet accepts every payload type (no registry is involved). An
// encoding/json marshal failure is returned as a [*csink.Error] with
// [csink.PhaseApply] and Outlet=="file".
//
// Optional capabilities:
//   - [csink.Flusher]: calls the underlying writer's Sync method when the
//     writer implements interface{ Sync() error } (e.g. *os.File), otherwise
//     Flush is a no-op.
//   - [csink.Shutdowner]: closes the file when the outlet owns one (i.e. was
//     created by [Open]). Shutdown is idempotent.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package file

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	csink "github.com/stablekernel/crucible/sink"
)

// writer is the narrow io.Writer surface this destination requires.
// It is satisfied by any io.Writer including *os.File, bytes.Buffer, etc.
type writer = io.Writer

// outlet appends JSON lines to a writer. It is safe for concurrent use.
type outlet struct {
	mu     sync.Mutex
	w      writer
	owned  io.Closer // non-nil when Open created the file
	closed bool
}

// New returns an [csink.Outlet] that marshals each payload as JSON and writes
// it as a single line (JSONL) to w. The outlet is safe for concurrent use.
// Any marshal failure is returned wrapped in a [*csink.Error].
func New(w io.Writer, _ ...Option) csink.Outlet {
	return &outlet{w: w}
}

// Open opens the file at path with O_APPEND|O_CREATE|O_WRONLY and returns an
// [csink.Outlet] that writes JSONL records to it. The outlet owns the file and
// will close it on [csink.Shutdowner.Shutdown].
//
// The caller should call Shutdown when done to release the file descriptor.
func Open(path string, opts ...Option) (csink.Outlet, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("file: open %q: %w", path, err)
	}
	o := &outlet{w: f, owned: f}
	_ = opts // reserved for future options
	return o, nil
}

// Option is a functional option for [New] and [Open]. No options are defined
// yet; the parameter is reserved for future extensibility.
type Option func(*outlet)

// Sink marshals payload to JSON and appends the result followed by a newline
// to the underlying writer. It is safe to call concurrently. A marshal failure
// is returned as a [*csink.Error]; an io.Writer error is returned directly
// (also wrapped as [*csink.Error]).
func (o *outlet) Sink(_ context.Context, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return &csink.Error{
			Outlet:      "file",
			Phase:       csink.PhaseApply,
			PayloadType: fmt.Sprintf("%T", payload),
			Err:         err,
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, err = fmt.Fprintf(o.w, "%s\n", data); err != nil {
		return &csink.Error{
			Outlet:      "file",
			Phase:       csink.PhaseApply,
			PayloadType: fmt.Sprintf("%T", payload),
			Err:         err,
		}
	}
	return nil
}

// Flush syncs the underlying writer when it implements interface{ Sync() error }
// (as *os.File does). For all other writers Flush is a no-op that returns nil.
// Flush is safe to call concurrently and is idempotent.
func (o *outlet) Flush(_ context.Context) error {
	type syncer interface{ Sync() error }
	o.mu.Lock()
	defer o.mu.Unlock()

	if s, ok := o.w.(syncer); ok {
		return s.Sync()
	}
	return nil
}

// Shutdown closes the underlying file when the outlet owns one (i.e. was
// created by [Open]). It is safe to call multiple times; subsequent calls
// return nil. For outlets created by [New], Shutdown is a no-op.
func (o *outlet) Shutdown(_ context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.owned == nil || o.closed {
		return nil
	}
	o.closed = true
	return o.owned.Close()
}
