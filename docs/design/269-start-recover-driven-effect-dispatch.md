# #269: domain effects of a parent Fire an actor spawned by Start or Recover drives

Status: open, needs a ruling on timing. The defect is pinned by
`TestStartAndRecover_DispatchTheEffectsOfAFireTheirSpawnsDrive`
(`durable/start_recover_driven_effects_test.go`, skipped pending this issue).

## The defect, with evidence

A transition an actor drives can emit a domain effect. When the actor was spawned by
a step — a `Fire`, a `Tick`, a delivery, a settle — that step dispatches the driven
Fire's effects as its own, under ids stamped after its own, and Recover's replay
re-fires the recorded Fire and re-dispatches under the same ids (#257,
`durable/actor.go` `drivenFireEffects`). When the actor was spawned by `Start` or
`Recover` itself, no step owns the Fire:

- The Fires sit pending on the Handle (`durable/runner.go:171`,
  `h.pending = startFires` at `:783`, `h.pending = recoverFires` at `:2297`).
- They journal as `before` entries of the *next* step (`appendStep`,
  `durable/runner.go:1642`–`1653`), so they precede that step's own work in the
  journal, the same order replay re-fires them in (`splitActorEntries`).
- No live path dispatches their effects, and Recover's replay arms them without
  sending them: `durable/runner.go:2590`–`2592` ("Their effects are armed but not
  returned: no live path dispatches them, so Recover sends none") and the matching
  code at `:2632`.

So the effect the transition emitted never reaches the handler, live or on recovery.
`TestRecover_ASpawnsFireArmsATimerAtItsRead` covers this path for timers, and
`durable/doc.go` carries the gap as a documented caveat ("a parent transition driven
by an actor that Start or Recover spawned isn't dispatched yet; see #269") — a fix
rewrites that sentence. The applies-once family pins these pending Fires' *state
changes* — `TestRecover_AppliesEachSpawnMessageOnce` (a spawn's message, by Start, Fire,
onDone or RunService, with and without checkpoints), `TestRecover_AppliesASpawnMessageOnce`
(a running actor's start message), and `durable/idle_tick_pending_test.go` (a pending
Fire's clock read across an idle Tick) — while `TestActorDrivenFire_EffectIDsPutTheStepsOwnFirst`
pins, with dispatch assertions, the own-first id rule a fix would extend, so an
implementation knows which assertions move; none of them dispatches a pending Fire's
effect. The skipped test pins the wanted end state: the effect dispatches exactly
once, live and on Recover, under the same id on both sides.

Three kinds of Fire reach `h.pending` this way: a failed spawn's declared `onError`, a
message a spawn sends its parent, and the settle of an actor that finishes as the
spawns run — a synchronous delivery during a spawn settling another running actor
(`state/actor_system.go:329`–`331` documents the settle case; `durable/runner.go:1331`–`1332`
names all three).

## Options

### Option A: dispatch with the next step, ahead of that step's own effects

The step that journals the pending Fires (the first step after `Start`/`Recover`)
dispatches their effects before its own, matching the journal's `before`-first order
and replay's re-fire order.

- Behavior: the effect lands with the first step's dispatch — a host that starts an
  instance and never runs another step (no Fire, Tick, delivery or settle) never sees
  the effect.
- API change: none. The dispatch machinery (`drivenFireEffects`,
  `dispatchableEffects`, `recordStep`) is internal.
- Cost: small — fold the pending Fires' effects into the step's dispatchable set,
  ahead of its own, and have `replayRecord` return the before-Fires' effects (in
  before-first order) so Recover re-dispatches under the same ids. The EffectID
  scheme's order rule gains a second case: today "the step's own effects come first,
  then each actor-driven parent Fire's" (`durable/effect.go:46`); a before-Fire's
  effects would come *first*, a new rule to document.
- Open sub-question: a crash between `Start` and the first step leaves the pending
  Fire unjournaled (it lives only in the Handle's memory). `Start`'s baseline records
  no ended/unstarted sets, so `Recover` spawns every declared actor fresh and the
  behavior re-runs — but `AbsorbRespawn` drops a *running* actor's start-driven
  messages (`state/actor_system.go:322`–`328`), so re-derivation hinges on the
  ended/unstarted sets at the last journal entry, which is the trace this question
  asks for, and the `Recover`-spawn window is the one to walk.

### Option B: dispatch at the moment the spawning Start or Recover returns

The pending Fires get a Record of their own, appended before `Start`/`Recover`
returns, so the effect is durable and dispatched at the spawning moment.

- Behavior: the effect lands as soon as the transition that emitted it is durable —
  the same moment a host Fire's effect lands.
- API change: none to exported API, but the journal gains a Record shape today's
  readers must tolerate: a Record whose entries are only `before` Fires, with no
  event of its own. That is a journal-shape change in unreleased durable, so no
  format gate is needed, but `StateAt`, `Load`-based tooling and the compaction
  policy all meet a new Record shape.
- Cost: a journal step per `Start`/`Recover` that has pending Fires (step numbering
  shifts — today's tests assert step numbers), and the crash window of Option A
  closes by construction.

### Option C: refuse or document, don't dispatch

Declare that a parent transition an actor spawned by `Start`/`Recover` drives must
not emit domain effects: either refuse at `Start`/`Recover` when it does, or
document the restriction.

- Behavior: the restriction is silent today; refusing makes previously-working
  machines fail at `Start`.
- API change: none.
- Cost: none to implement, but it leaves a real seam undelivered — an actor that
  greets its parent with a side effect is a natural machine, not a misuse.

## Recommendation

Option A. It is the smallest change, it matches the journal's and replay's existing
before-first order (the issue's own comment notes that folding them into the step's
effects passes the whole suite today — evidence only that nothing regresses, not that
the seam works), and it needs no new Record shape. The EffectID order rule change and
the crash-window trace above should be settled with it.

## Questions needing a decision

1. Timing: Option A, B, or C — Temple's call; the issue leaves exactly this open.
2. Under Option A, does the crash-between-Start-and-first-step window re-derive
   every pending Fire from the respawn, or can an effect be lost there? The trace
   must walk the `AbsorbRespawn` quiet-drop rule (`state/actor_system.go:322`–`328`)
   against the ended/unstarted sets the last journal entry records.
3. Under Option A, the EffectID order rule gains "before-Fires' effects first": is
   that ordering acceptable, given it inverts the own-first rule for every other
   driven Fire?
4. Should the pinning test's flavors grow to cover the third driving path (the
   settle of an actor that finishes as the spawns run) and a `Recover`-spawned
   actor driving a *new* Fire (not a replayed one), both named by the issue but
   deferred here — and a leg that dispatches live, then recovers with the handler
   still wired, asserting the total stays one across the live-to-Recover sequence?
