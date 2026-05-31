# Migration guide: `state` v0.2.0 → v1.0.0

v1.0.0 freezes the data model and contracts. Most of the surface is unchanged —
`Forge`/`Temper`/`Quench`/`Cast`/`Fire`/`Assay`, the builder DSL, guards, actions,
services, actors, `after`, history, snapshots, and the IR all behave as before.
This guide covers only the breaking changes a consumer must address. They are
listed in rough order of effort.

## 1. Context is value-semantic; move context writes into assign reducers

This is the central change. Context (`C`) now flows through a step as data:
guards and actions observe it through a **read-only** projection, and the **only**
place context changes is an **assign reducer**. Actions emit effects; they can no
longer mutate context.

If you previously mutated context through a pointer inside an action, move that
write into an `AssignFn` — a total pure reducer that takes the prior context by
value and returns the next context — registered with `Registry.Assign` (alias
`Builder.Reducer`) and wired onto the transition with `Builder.Assign(name)`.

**Before** — an action mutates the context in place:

```go
reg.Action("recordHold", func(in state.ActionCtx[Order]) (state.Effect, error) {
    in.Entity.AuthHold = "held"                    // mutates context via pointer
    in.Entity.Log = append(in.Entity.Log, "authorized")
    return nil, nil
})

// ... wired as an effect-producing action:
Transition(Authorizing).On(Authorized).GoTo(Active).Do("recordHold")
```

**After** — a reducer returns the next context; an action (if any) only emits an
effect:

```go
func recordHold(in state.AssignCtx[Order]) Order {
    c := in.Entity                                 // prior context, by value
    if hold, ok := in.Event.(string); ok {
        c.AuthHold = hold
        c.Log = append(c.Log, "authorized:"+hold)
    }
    return c                                        // next context
}

reg.Assign("recordHold", recordHold)               // alias: Builder.Reducer

// ... wired with .Assign on the transition:
Transition(Authorizing).On(Authorized).GoTo(Active).Assign("recordHold")
```

Notes:

- The kernel folds the assigns declared on a transition's **exit, transition, and
  entry** phases — in that order, declaration order within each phase, each seeing
  the prior result — and the folded value becomes the instance's context at commit.
- The triggering event is in scope as `AssignCtx.Event`. For a service or actor
  `onDone` transition the host re-fires the routing event with the result as its
  payload, so the result reaches the reducer through `AssignCtx.Event` — no host
  side channel.
- Use `Builder.OnEntryAssign` / `Builder.OnExitAssign` to attach a reducer to a
  state's entry/exit rather than a transition.
- Reducers are total: an `AssignFn` returns only `C` (no error). A panic is
  recovered like a guard panic.
- The DSL verb appears in two roles: `Registry.Assign` / `Builder.Reducer`
  **register** a reducer impl under a name; `Builder.Assign(name)` **wires** a
  registered reducer onto a transition.

## 2. `ActionResult.ContextDelta` is removed

The previously-reserved `ContextDelta` slot on the action result is gone. Under
the value-semantics contract a context change is the value a reducer returns
(`AssignResult.Context`), not a delta carried back from an action. Drop any
reference to `ActionResult.ContextDelta` and move the write into an assign
(section 1).

## 3. Built-in effects serialize with lower-camel keys; the `EffectsEmitted` suffix is the stable `Kind`

The built-in effect structs (`SpawnActor`, `StopActor`, `StartService`,
`StopService`, `ScheduleAfter`, `CancelScheduled`, `SendTo`, `SendParent`,
`RespondToSender`, `ForwardEvent`) now carry JSON field tags, so their serialized
form is lower-camel and stable:

```json
{ "id": "...", "src": "...", "state": "..." }
```

rather than the Go field names. And a `Trace.EffectsEmitted` label now records an
effect's stable **`Kind`** in place of its Go type name. (The `name:…` ref prefix
is unchanged, so conformance ref-name assertions are unaffected.)

- If you serialized a built-in effect struct directly, update consumers to the
  lower-camel keys.
- If you parsed the **type-name suffix** of an `EffectsEmitted` label, switch to
  matching the effect's stable `Kind`.
- Type-switching on the effect structs in Go is **unaffected** — the structs only
  gained methods and JSON tags.

## 4. `WithAggregate` → `Aggregate`

The `Assay` option that collects all failing requirements in one pass (instead of
failing fast) is renamed.

```go
// Before
report := state.Assay(m, state.WithAggregate())

// After
report := state.Assay(m, state.Aggregate())
```

## 5. Unhandled child-actor failure now escalates to the parent

A child actor that fails with **no `onError` route** previously had its failure
swallowed silently. It now **escalates to the parent**. This fixes a silent-crash
footgun, but a host that relied on the old swallow must wire one of:

- An `onError` route on the actor's `Invoke`/`Spawn` (preferred — handle the
  failure as a transition):

  ```go
  InvokeActor("kitchen", PlatedUp, KitchenFailed /* onError */)
  ```

- An escalation handler on the `ActorSystem` to observe/route escalations:

  ```go
  sys.WithEscalationHandler(func(ctx context.Context, esc *state.ActorEscalation) {
      // log, alert, or restart per your supervision policy
  })
  ```

- Or read the most recent escalation off the system after stepping it:

  ```go
  if esc := sys.LastEscalation(); esc != nil {
      // inspect esc
  }
  ```

A registered inspector also observes every escalation.

---

For the full list of additions in this release (the versioned IR envelope, the
context schema, the graduated guard tiers, the determinism/ordering contract, and
the snapshot-version and journal seams), see [CHANGELOG.md](./CHANGELOG.md).
