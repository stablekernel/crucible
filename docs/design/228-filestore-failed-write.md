# #228: FileStore can lose or double-apply steps after a failed write or crash

Status: open, needs a ruling on the recovery model. The defects are pinned by
`durable/filestore_failed_write_test.go` (all four tests skip pending this issue,
each demonstrated failing before its skip was added).

## The defects, with evidence

Three findings from the issue, plus its follow-up comment; all contradict the
Atomicity contract in FileStore's doc (`durable/filestore.go:41`–`49`: "a successful
Append is durable", "a concurrent reopen never observes a checkpoint torn against
its tail").

1. **A torn trailing journal line loses acknowledged steps.** `appendLine`
   (`durable/filestore.go:617`–`632`) never checks the file's last byte, so after a
   failed write leaves a newline-less fragment, the next `Append` merges into it and
   returns success (`:422`). On reopen the merged line fails to parse and
   `loadJournal` skips it (`:310`), so the acknowledged step is gone; a retry of it
   reports it as applied, because the ledger holds its key (`:399`). No crash is
   needed. `TestFileStore_TornTrailingRecord_RecoversCleanly` never reopens after
   the merge, so it misses this.
2. **`checkpoint.snap` and `checkpoint.meta` aren't atomic together.** `Checkpoint`
   renames the snapshot (`:511`) then the meta (`:514`), so a kill between the two
   pairs the new snapshot with the old `throughStep`; a failed meta write before
   its rename does the same with no kill at all. `Load` then returns that snapshot
   beside a tail of steps it already holds, and replay double-applies them (or
   fails an invalid transition). No rename order fixes the pairing: meta-first
   drops steps instead. The in-process retry heals, but any reopen in the window
   observes the split pair.
3. **The journal and the applied ledger can disagree.** `Append` writes the journal
   (`:422`) then the ledger (`:431`), and reopen seeds the ledger from the ledger
   file alone (`loadApplied`, `:291`):
   - a kill between the two leaves a journaled step with no ledger key, so the
     retry — the idempotent no-op the Store contract promises — returns
     `ErrStepOutOfOrder` (`:403`);
   - a journal write whose bytes land but whose `Sync` fails leaves the same disk
     state with no kill: a complete line no ledger key covers, and no in-memory
     state either, so the retry duplicates the step — a path finding 3's failed
     *ledger* write mirrors;
   - a failed ledger write, with no crash, leaves the step out of the in-memory
     ledger too, so a retry appends the journal again — the same record, or a
     different one for the same step (the follow-up comment's case), which the
     #229 conflict check can't catch because it reads the ledger.

   A sibling of finding 2's window: if the compaction rewrite fails after both
   renames landed, `Checkpoint` returns an error while the new snapshot and
   throughStep are live on disk. Reload is consistent (`loadJournal` drops records
   at or below throughStep), and the Handle's own retry in the same process heals —
   it carries a landed-after-all branch for a re-checkpoint refused with
   `ErrCheckpointNotAdvancing` (`durable/runner.go:1609`–`1612`, machinery from
   #241's fix, though #241's tests inject a one-shot failure rather than that
   refusal, and with the reference stores the branch may be unreachable
   in-process) — but after a reload, a retried `Checkpoint` at the same step is
   refused with `ErrCheckpointNotAdvancing`, so a caller that reopens between the
   failure and the retry holds a lying error.

The same torn-line class reaches `dispatched.log` (`loadDispatched`, `:326`): a torn
trailing id line silently loses the mark, so a recovered effect dispatches again.
The issue doesn't name it; a fix should decide whether it's in scope.

## Options

### Option A: the journal is the source of truth; repair and reconcile to it

- `appendLine` guards the trailing byte: if the file's last byte isn't a newline,
  truncate to the last intact line before appending (one extra seek/read per
  append), so an Append onto a torn tail never acks a merged line.
- On load, `loadApplied` seeds the default `step:<n>` keys (and digests, from the
  line bytes) from the journal when the ledger lacks them, and `loadJournal`
  refuses a journal holding two records for one step — that state is corruption,
  not something to silently pick from.
- `Append` updates its in-memory ledger from the journal bytes as soon as they
  land — after the Write, whatever `Sync` then reports (the follow-up comment's
  second suggestion, made byte-precise) — so a failed ledger write or a
  Sync-failed journal write leaves the retry the idempotent no-op rather than a
  second journal line. Trusting unflushed bytes is the trade: a crash can still
  lose the line, and the load-side seeding below covers that reopen. The ledger
  becomes a cache whose only job is surviving compaction.
- `Checkpoint` writes one file — snapshot and `throughStep` in a single
  atomically-renamed file — so the pair cannot be split. Layout change: the two
  `checkpoint.*` files become one. The two-file layout predates durable's recorded
  0.1.0 (`durable/CHANGELOG.md:656`, whose link names a `durable/v0.1.0` release no
  tag in this repository carries), so the old shape is release-visible, not just a
  dev artifact — whether a load must still read it is Q2's question, not a given.
- Behavior: every failure mode above becomes either correct or a loud refusal.
- API change: none exported; the on-disk layout changes.
- Cost: one seek+read per append; a load that walks the journal twice (or once,
  collecting both); a new single-file checkpoint format.

### Option B: ledger-first ordering, or the in-memory ledger after the journal write

The follow-up comment's first suggestion: the ledger write lands before the journal
sync; or its second: the in-memory ledger updates once the journal is synced.

- Behavior: a failed journal write leaves a ledger key with no journal line, so a
  retry of the same record hits the idempotent check and reports *applied* while
  the journal can't produce it — the mirror image of finding 1, unless the check
  also verifies the journal. The torn-tail merge (finding 1) and the split
  checkpoint (finding 2) are untouched.
- API change: none.
- Cost: small, but it closes one window by opening another, so it needs Option A's
  journal-side checks anyway to be sound.

### Option C: fail loud — poison the instance after any write failure

After a failed journal or ledger append, or a failed checkpoint write, the instance
refuses further Appends and Checkpoints until the store is reopened (and the reopen
reconciles what it can).

- Behavior: no silent loss in-process; the crash windows (kill between writes)
  remain, so findings 1–3 still occur across a crash — the poison only stops the
  same-process follow-on writes.
- API change: none.
- Cost: smallest; leaves the crash cases to whatever lands later.

## Recommendation

Option A, with B's in-memory-ledger-after-journal-write folded in (it is A's third
bullet). The journal is already documented as the source of truth for the tail; the
ledger and the checkpoint pair are derived state, and every defect above is a place
where derived state is trusted over the truth. The single-file checkpoint and the
old-shape load path are the pieces that need a ruling before the fix PR the issue
promises.

## Questions needing a decision

1. Recovery model: repair-and-reconcile (A), reorder (B), or fail-loud (C) —
   Temple's call; the issue's Notes lean toward A's load half without settling the
   write half.
2. Is the on-disk layout change (one checkpoint file) acceptable, and must a load
   still read the two-file shape existing dev directories hold?
3. When a journal holds two records for one step (the follow-up comment's case,
   already on disk), should `Load` refuse, or is a repair tool in scope?
4. An explicit `WithIdempotencyKey` isn't recoverable from the journal, so A's
   load-side seeding covers default `step:<n>` keys only. The realistic custom-key
   retry lands at a *fresh* step (custom keys dedup independent of step), so the
   double-apply is two journal records at different steps under one key — invisible
   to the same-step duplicate check. Should idempotence for custom keys compare the
   key's journal lines instead, and should a pin cover it?
5. Is the `dispatched.log` torn-line case (a lost mark re-dispatches an effect) in
   this fix's scope, or tracked separately?
6. Is the failed-compaction window above (a `Checkpoint` error whose checkpoint
   took) in scope, or accepted as benign?
