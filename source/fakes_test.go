// SPDX-License-Identifier: Apache-2.0

package source_test

import "github.com/stablekernel/crucible/source"

// testMsg is a minimal in-package Message for unit tests that exercise the core
// types directly (codec, middleware) without the memsource machinery.
type testMsg struct {
	key          []byte
	value        []byte
	headers      source.Headers
	subject      string
	partitionKey string
}

func (m testMsg) Key() []byte             { return m.key }
func (m testMsg) Value() []byte           { return m.value }
func (m testMsg) Headers() source.Headers { return m.headers }
func (m testMsg) Subject() string         { return m.subject }
func (m testMsg) PartitionKey() string    { return m.partitionKey }
func (m testMsg) Cursor() source.Cursor   { return testCursor(m.subject) }
func (m testMsg) As(any) bool             { return false }

type testCursor string

func (c testCursor) String() string { return string(c) }
