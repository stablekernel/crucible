# #253: an instance touched by two versions in a rolling deploy can't be recovered by either

Status: open, needs a ruling on the operator path. The pinnable defect is pinned by
`TestRecover_AMixedVersionJournalRefusesBeforeAnyEffectIsSent`
(`durable/rolling_deploy_recovery_test.go`, skipped pending this issue, demonstrated
failing before its skip was added).

## The defect, with evidence

Since the format gate landed with the lost-`Record.Format` fix
(`durable/CHANGELOG.md:408`–`411`; the issue's text cites #235, but that PR was the
effect-dispatch change — the number in the issue looks like a slip worth
confirming), a checkpoint records the journal format of the version that wrote it,
and a Record after one with a lower format fails replay (`checkFormat`,
`durable/runner.go:2673`; the tail loop tests each Record in turn at `:2059`). An
older binary refuses a Record whose format is lower than its checkpoint too, so in a
rolling deploy — an instance a new node starts, an old node then fires on — neither
version recovers the instance, and nothing documents a way back short of editing the
journal by hand.

On the way to that refusal, Recover still does work it can't stand behind: the format
check runs per Record inside the tail loop, after earlier Records have replayed *and*
re-dispatched their domain effects (`dispatchReplayEffects` at `durable/runner.go:2086`).
The pinned test shows one notify landing before the refusal — the issue's comment
names exactly this ("a mixed-version journal that `Recover` refuses still dispatches
the effects of the Records before the refusal. ... Whatever fixes this issue should
refuse before any effect is sent"). `StateAt` shares the tail walk but dispatches
nothing (`durable/timetravel.go:276` runs `replayRecord` with no effect re-dispatch),
and the stateless `Runner.Fire` routes through the same `recover`
(`durable/runner.go:1215`–`1227`), dispatching when a handler is wired — so the gap
is Recover's and the stateless Fire's, and the pin names only Recover.

#253 now also tracks the pre-release compatibility items — #276 (replay decodes a
service result with the recovering build's decoder), #259 (an upgraded instance whose
older tick left a service in flight), #262 (an older journal's misplaced settle clock
read) — closed into it to be decided together before durable's first release.

## Options

### Option A: an operator recovery path that truncates at the version boundary

A `Recover` option, or an offline tool, that truncates the tail at the first Record
from the other version and recovers from the steps up to it.

- Behavior: the instance comes back at the last state this version can vouch for;
  the other version's steps are dropped — they are unrecoverable by either version
  anyway, so the loss is the truth, not a new one. The truncation must refuse before
  any effect is sent (the comment's requirement, which is also a prerequisite below),
  and it must leave a journal a later plain `Recover` accepts — an immediate
  re-checkpoint under the fence, or a recorded boundary marker.
- API change: yes, exported — a `Recover` option (name and shape need ratifying) or a
  new `cmd/` tool; an operator-facing destructive action also wants a dry-run form.
- Cost: a truncating write path — or none new, if the re-checkpoint route is taken,
  since `Store.Checkpoint` already compacts the journal through a step; the fence
  matters either way, since two operators running it concurrently must not
  interleave truncations.

### Option B: deploy guidance only

Document the "don't run mixed versions on one instance" stance the issue attributes
to #235's docs — drain or pin an instance to one version during a rollout — which the
repo's own documentation doesn't carry today, so the guidance is new writing, not a
pointer.

- Behavior: nothing changes in code; an operator who hits the mixed journal anyway
  still has only hand-editing — the situation the issue opened with.
- API change: none.
- Cost: none. The guidance should land regardless of the ruling; it isn't sufficient
  alone, or the issue wouldn't exist.

### Option C: a format-compatibility window

Version N also writes format N-1 — records and checkpoints — until a flag flips, so a
mixed window doesn't refuse.

- Behavior: no refusals during the window, but no protection either: the format gate
  exists because a binary replaying a Record under the wrong version's semantics
  recovers silently wrong (the CHANGELOG entry's own rationale — a Store that lost
  `Record.Format` replayed as an older journal's and silently skipped armed timers,
  `durable/CHANGELOG.md:408`–`411`), so the window trades a loud refusal for that
  silent wrong recovery. And binaries already built can't be changed: the window
  protects only flag-era builds on both sides of it, so a rollout already under way
  keeps the gaps below.
- API change: the flag is exported configuration; its flipping semantics need
  ratifying.
- Cost: every format bump from then on carries the previous format's writer, and the
  window's safety depends on the two formats' replay semantics agreeing — an argument
  that has to be re-made at every bump.

### The piece every option needs first: refuse before any effect is sent

An upfront check of the whole journal — before replay, before any dispatch — so a
mixed journal costs nothing but the refusal. This is settled by the issue's
comment, is internal to the Runner (no exported API), and the pinned test is its
acceptance test. The check's direction rules are the design content:

- a Record below the checkpoint's format refuses, as today — the forward direction
  the pinned test covers;
- a Record above the checkpoint's format but at or below the running version's
  `journalFormat` is the supported in-place upgrade (a new binary recovering an old
  journal), and must keep passing — `TestCheckFormat_HoldsRecordsToTheCheckpointFormat`
  pins that a higher format is accepted today;
- the check compares each Record against the *checkpoint's* format, not a running
  maximum: a journal an old binary checkpoints at format 2, a new binary appends to
  (Records at 3), and the old binary fires on again (Records at 2) never refuses on
  either binary — every Record clears the checkpoint's own number;
- nothing today refuses a *future* journal: a checkpoint or Record above the running
  version's `journalFormat` replays without complaint (`unmarshalCheckpoint` tolerates
  any envelope, and no comparison in the package refuses a format above
  `journalFormat` — the one at `:2603` refuses a mismatched tick count, not the
  format, and the others test `<`),
  so an already-deployed older binary silently replays a newer version's Records —
  the same silent wrong recovery, in the reverse direction, that the issue's window
  is about. The upfront check should refuse it, which narrows the pinned
  accepts-a-higher-one rule to "at or below the running version's format" — a change
  to an existing pin that needs the ruling's blessing.
- a bare pre-envelope checkpoint reads as format 0, so a check keyed to the
  checkpoint's format alone never refuses its tail; keying the future-format rule to
  the running version's `journalFormat` covers that hole too.

## Recommendation

Land the upfront refusal's below-checkpoint rule (it needs no ruling beyond the one
the comment already gives; its future-format rule and the pin it narrows are
question 6's to bless), ship Option B's guidance with it, and take Option A as the
operator path —
C's silent-wrong-recovery trade is the one failure mode worse than the status quo.
A's exported surface is exactly what needs the ruling: option versus tool, and the
dry-run form.

## Questions needing a decision

1. The operator path: A (option or tool?), B alone, or C — Temple's call; the issue
   lists all three without settling one.
2. If A: a `Recover` option or an offline `cmd/` tool? An option is discoverable
   where the failure surfaces; a tool keeps the `Recover` API surface clean and makes
   the destructive step deliberate. Either is new exported surface to ratify.
3. Truncation boundary: the first Record of a *lower* format only, or any Record
   whose format differs from the checkpoint's in either direction (a newer version's
   Record on an old checkpoint is the reverse window)?
4. After truncation, what makes the journal plain-recoverable again — an immediate
   re-checkpoint under the fence, or a recorded boundary marker the loader honors?
5. Are the folded pre-release items (#276, #259, #262) decided in this ruling or
   tracked to their own, once the recovery path lands?
6. Does the upfront refusal's future-format rule — refusing a checkpoint or Record
   above the running version's `journalFormat`, and keying the lower-format rule to
   a running maximum rather than the checkpoint alone, so a tail that descends
   behind a same-number checkpoint refuses — have the ruling's blessing, given it
   narrows `TestCheckFormat_HoldsRecordsToTheCheckpointFormat`'s accepted-higher
   case to the in-place-upgrade range? And should the pin grow a leg for the
   stateless `Runner.Fire` and one for the reverse direction?
7. The same-format mixed-version class the repo already tracks under #253 — a
   checkpoint and Records that share a format number yet were written by builds on
   either side of a behavior change, such as the StartEffects widening
   (`durable/CHANGELOG.md:96`–`103`) — sails through any format-keyed check. Is
   that class in this ruling's scope, or does it wait for the compatibility-window
   decision the folded items belong to?
