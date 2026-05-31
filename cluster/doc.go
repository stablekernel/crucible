// Package cluster is the host-side distribution runtime for the Crucible state
// kernel. It lets a parent machine on one node address and drive a child-machine
// actor running on another node, supervises actor failures with restart and
// backoff strategies, and migrates a running instance between nodes — all over a
// pluggable transport, with the kernel left pure and stdlib-only.
//
// The package is additive over the state kernel. It consumes the kernel's
// already-reserved seams — the opaque ActorRef (whose Node locator names the
// owning host), the injectable ActorSystem host-driver, the Snapshot / Restore
// pair, and the typed ActorEscalation / EscalationHandler — without requiring any
// change to the kernel beyond the additive ActorRef.Node locator.
//
// # System
//
// System wraps a local state.ActorSystem and a node identity into a distributed
// actor system. Delivery to a ref the local node owns (an empty Node, or a Node
// equal to this node) is delegated straight to the wrapped ActorSystem; delivery
// to a ref another node owns is routed over the Transport. A System with no
// Transport configured serves local actors transparently and reports
// ErrNoTransport for a remote ref, so the in-process projection of the actor
// model keeps working unchanged.
//
// # Transport
//
// Transport is the seam that moves an actor operation (deliver, spawn) to the node
// that owns the target actor. It is host-supplied, so the kernel and this package's
// core carry no network dependency: the in-tree InMemoryTransport drives multi-node
// tests and single-process development, and a real network transport (gRPC) lives
// behind the same interface.
//
// # Supervision
//
// Supervisor turns the kernel's typed ActorEscalation into a per-source supervision
// policy. Each failed actor is routed to a Decision by the src it was spawned from:
// Escalate forwards the failure to a sink up the hierarchy, Stop contains it,
// Restart re-spawns the actor through a Respawner within a per-source budget, and
// Backoff defers the re-spawn behind an exponentially growing delay applied by the
// host through Tick. It plugs into the seam with ActorSystem.WithEscalationHandler.
//
// # Migration
//
// Capture snapshots a running instance, its actor tree, and its machine definition
// into a wire-shippable Checkpoint; Restore rebuilds it on another node, resuming in
// place. Restore gates the move on schema compatibility — it diffs the source and
// target machine definitions with state/evolution and refuses a breaking target with
// ErrIncompatibleMigration — so an instance never resumes against a definition that
// would misread its persisted configuration.
package cluster
