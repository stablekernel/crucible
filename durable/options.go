package durable

// This file defines the functional-options surface for the Store operations and
// for MemStore construction. Required arguments stay positional; every new
// capability arrives as an additive ...Option so a signature never has to break
// to grow. The options here are the extension seams the later runner builds on
// (per-append idempotency keys, per-checkpoint compaction policy); they default
// to no-ops so the contract holds without any option supplied.

// AppendOption configures a single Store.Append call. It is the additive
// extension point for per-append behavior the durable runner layers on later
// (for example, an explicit idempotency key distinct from the Record's Step).
type AppendOption func(*appendConfig)

// appendConfig holds resolved AppendOption state for one Append.
type appendConfig struct {
	// idempotencyKey, when set, overrides the Record Step as the dedup key for
	// the append. Empty means dedup on Step alone (the default contract).
	idempotencyKey string
}

// WithIdempotencyKey sets an explicit idempotency key for an Append, overriding
// the default of deduplicating on the Record's Step alone. Two Appends carrying
// the same key for the same instance collapse to one. An empty key is ignored
// (the Step-based default applies).
func WithIdempotencyKey(key string) AppendOption {
	return func(c *appendConfig) {
		if key != "" {
			c.idempotencyKey = key
		}
	}
}

func resolveAppend(opts ...AppendOption) appendConfig {
	var c appendConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// CheckpointOption configures a single Store.Checkpoint call. It is the additive
// extension point for per-checkpoint policy the durable runner layers on later
// (for example, retaining the compacted tail for time-travel reads instead of
// discarding it).
type CheckpointOption func(*checkpointConfig)

// checkpointConfig holds resolved CheckpointOption state for one Checkpoint.
type checkpointConfig struct {
	// retainTail keeps the pre-checkpoint Records instead of compacting them
	// away, so a later time-travel reader can replay through earlier steps.
	retainTail bool
}

// WithRetainTail keeps the Records a Checkpoint would otherwise compact away, so
// a later time-travel reader can still replay through the pre-checkpoint steps.
// The default compacts the tail through the checkpoint to bound storage growth.
func WithRetainTail() CheckpointOption {
	return func(c *checkpointConfig) { c.retainTail = true }
}

func resolveCheckpoint(opts ...CheckpointOption) checkpointConfig {
	var c checkpointConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// MemStoreOption configures MemStore construction. It keeps NewMemStore
// extensible — new construction-time knobs arrive as additive options rather
// than new positional parameters.
type MemStoreOption func(*memStoreConfig)

// memStoreConfig holds resolved MemStoreOption state.
type memStoreConfig struct {
	// initialCapacity pre-sizes the per-instance Record slice to reduce
	// reallocation for instances with a known step count.
	initialCapacity int
}

// WithInitialCapacity pre-sizes each instance's Record buffer to the given
// number of steps, trading a little memory for fewer reallocations on
// instances with a known step count. A non-positive value is ignored.
func WithInitialCapacity(steps int) MemStoreOption {
	return func(c *memStoreConfig) {
		if steps > 0 {
			c.initialCapacity = steps
		}
	}
}

func resolveMemStore(opts ...MemStoreOption) memStoreConfig {
	var c memStoreConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}
