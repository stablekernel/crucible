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
	// fence is the writer's fence token. Zero means no token was supplied.
	fence int64
}

// WithIdempotencyKey sets an explicit idempotency key for an Append, overriding
// the default of deduplicating on the Record's Step alone. Two Appends carrying
// the same key for the same instance collapse to one, with no comparison of the
// Records. An empty key is ignored (the Step-based default applies). Keys starting
// "step:" are reserved for the Store's own per-Step key.
func WithIdempotencyKey(key string) AppendOption {
	return func(c *appendConfig) {
		if key != "" {
			c.idempotencyKey = key
		}
	}
}

// WithFence carries the writer's fence token on an Append: an increasing number,
// issued by the host with each ownership of the instance. A Store refuses a token
// lower than the highest it has seen for the instance with ErrFenced (see Store). A
// non-positive token is the same as no token, which a fenced instance refuses.
// WithCheckpointFence is the Checkpoint counterpart; a Runner sets both from
// WithOwnerFence.
func WithFence(token int64) AppendOption {
	return func(c *appendConfig) {
		if token > 0 {
			c.fence = token
		}
	}
}

// AppendIdempotencyKey returns the explicit key opts carry, or "" when there is
// none. It is how a Store backend outside this package honors WithIdempotencyKey.
func AppendIdempotencyKey(opts ...AppendOption) string {
	return resolveAppend(opts...).idempotencyKey
}

// AppendFence returns the fence token opts carry, or zero when there is none. It
// is how a Store backend outside this package reads the token.
func AppendFence(opts ...AppendOption) int64 {
	return resolveAppend(opts...).fence
}

func resolveAppend(opts ...AppendOption) appendConfig {
	var c appendConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// CheckpointOption configures a single Store.Checkpoint call. It is the additive
// extension point for per-checkpoint policy a backend may layer on, so that policy
// can arrive without breaking the Store interface. Time-travel retention,
// which an earlier per-checkpoint flag covered, is now a store-level capability
// (the HistoryStore seam, MemStore's WithHistory).
type CheckpointOption func(*checkpointConfig)

// checkpointConfig holds resolved CheckpointOption state for one Checkpoint.
type checkpointConfig struct {
	// fence is the writer's fence token. Zero means no token was supplied.
	fence int64
}

// WithCheckpointFence carries the writer's fence token on a Checkpoint, with the
// same meaning as WithFence on an Append. A non-positive token is the same as no token, which a fenced instance refuses.
func WithCheckpointFence(token int64) CheckpointOption {
	return func(c *checkpointConfig) {
		if token > 0 {
			c.fence = token
		}
	}
}

// CheckpointFence returns the fence token opts carry, or zero when there is none.
// It is how a Store backend outside this package reads the token.
func CheckpointFence(opts ...CheckpointOption) int64 {
	return resolveCheckpoint(opts...).fence
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
	// history retains each instance's start baseline snapshot and full ordered
	// Record log (surviving checkpoint compaction) so the time-travel reader can
	// reconstruct any recorded step. It implements the HistoryStore seam.
	history bool
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

// WithHistory makes a MemStore retain each instance's start baseline snapshot and
// full ordered Record log, even across checkpoint compaction, so the time-travel
// reader (StateAt) can reconstruct the instance's state as of any recorded step. It
// implements the HistoryStore seam. The default discards compacted Records to bound
// memory; enable this for audit, debugging, or replay-inspection workloads.
func WithHistory() MemStoreOption {
	return func(c *memStoreConfig) { c.history = true }
}

func resolveMemStore(opts ...MemStoreOption) memStoreConfig {
	var c memStoreConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// FileStoreOption configures FileStore construction. It keeps NewFileStore
// extensible — new construction-time knobs (on-disk encoding, sync policy,
// retention) arrive as additive options rather than new positional parameters.
type FileStoreOption func(*fileStoreConfig)

// fileStoreConfig holds resolved FileStoreOption state. It is presently empty:
// the working baseline (per-record fsync durability, atomic checkpoints) needs no
// tuning, and the type reserves a stable seam for additive options.
type fileStoreConfig struct{}

func resolveFileStore(opts ...FileStoreOption) fileStoreConfig {
	var c fileStoreConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
}
