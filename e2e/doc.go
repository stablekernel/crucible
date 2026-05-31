// Package e2e holds whole-stack integration tests that wire the Crucible suite's
// modules together — the kernel (state), rich expressions (state/expr), durable
// execution (durable), distribution (cluster), and the gRPC transport (transport)
// — and exercise the compositions the per-module tests cannot reach on their own.
//
// It carries no production code: it is a test-only module so that depending on
// every other module (and their transitive dependencies, including gRPC) never
// leaks into any shipping module's dependency graph. Each test combines two or more
// modules in a realistic scenario — a CEL-assign-driven durable instance that
// recovers, a supervised actor spawned and driven across nodes over real gRPC, and
// distributed time-travel of a durable instance — proving the seams compose.
package e2e
