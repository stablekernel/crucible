# Food-delivery example

A complete, runnable order-lifecycle statechart built with Crucible. It models a
generic food-ordering flow (place an order, hold payment, cook, deliver, capture)
and exercises the whole engine at once: hierarchy and parallel regions, actors,
invoked services, timed deadlines, a compensation saga, and snapshot/restore across a
restart.

This is the flagship example: it is written the way Crucible recommends you write a
v1 machine. Read it as a tutorial for the recommended patterns, then lift the shapes
into your own domain.

## The domain

An `Order` moves through these stages:

```
Placed --Submit--> Authorizing
  Authorizing invokes the payment "authorize" service.
    on decline --> Rejected (terminal)
    on success [admit guard] --> Active
Active  (parallel superstate)
  ├─ Fulfillment region: Cooking --> AwaitingCourier --> EnRoute
  │     Cooking supervises the kitchen actor (it plates the meal).
  │     EnRoute  supervises the courier actor (it delivers).
  └─ Watchdog region:    OnTime --after(SLA)--> Overdue (records a breach)
  Active --DroppedOff--> Settling   (courier completion exits the parallel state)
  Active --Cancel-----> Refunding   (the compensation saga)
Settling --always--> Delivered (terminal)   (captures the payment hold)
Refunding invokes "refund"; on its result --> Canceled (terminal)
```

Run it through the host `Rig` (the example's wiring of the Scheduler, ServiceRunner,
and ActorSystem); see `rig.go`, or embed the pieces directly.

## Requirement → engine feature

Each business requirement maps to one engine capability. This is the table to study.

| Business requirement | Engine feature | Where to look |
|---|---|---|
| "An order has data (basket, tip, payment hold) that changes as it progresses." | **Value-semantics context** (`Order` passed and returned by value). | `Order` struct; `m.Cast(order, ...)` in `rig.go`. |
| "Only well-defined steps may change that data; nothing mutates it behind your back." | **Assign reducers** are the *sole* context writers. Guards and the kernel see the context read-only. | every `Reducer(...)` and `.Assign(...)`; `recordHold`, `settleReducer`, etc. |
| "Admit an order to fulfillment only if it clears a business rule." | **Core guard expressions** over a **ContextSchema** (typed compare + membership, evaluated in-kernel, type-checked at build). | `WithContextSchema(SchemaOf[Order]())`; `Field(...).Ge(...)`, `Field(...).In(...)` on the `Authorized` edge. |
| "Some rules are richer than compare-and-match." | **Rich (CEL) guard** from `state/expr`, composed with the Core leaves via `Or`. | `expr.Guard(...)` building `generousOrder`; the `WhenExpr(Or(generous, And(...)))`. |
| "Cooking and delivery happen alongside a delivery-time watchdog." | **Hierarchy + parallel regions** (an orthogonal `Active` superstate with two regions). | `SuperState(Active)` with `Region("Fulfillment")` ∥ `Region("Watchdog")`. |
| "The kitchen and the driver are independent participants that report back." | **Actors**: child machines the order supervises; their output returns through the completion event. | `Actor("kitchen")`, `Actor("courier")`; `kitchenBehavior`, `courierBehavior`. |
| "Hold the payment, then capture it; reverse it if the order is canceled." | **Invoked services** with `onDone`/`onError`. | `Service("authorize", ...)`, `Service("refund", ...)`; `Invoke(...)`. |
| "A service's result must flow into the order's data." | **onDone-via-event**: the host re-fires the routing event carrying the result; the reducer reads it from `AssignCtx.Event`, no side channel. | `recordHold`, `recordRefund`, `recordPrep`, `recordDrop`. |
| "Flag an order that misses its delivery window." | **`after` / SLA timeout** via the Scheduler, driven deterministically with a fake clock in tests. | `After(SLAWindow)` in the Watchdog region; `Rig.BreachSLA`. |
| "Canceling a paid order must reverse the charge." | **Compensation saga**: a post-payment cancellation invokes the refund and folds the reversed amount in. | the `Cancel` cross-cutting transition → `Refunding` → `recordRefund`. |
| "Survive a process restart mid-order." | **Snapshot → restore**: the value context, active parallel configuration, pending timers, and in-flight actors all round-trip. | `Rig.Snapshot`, `RestoreRig`; `TestScenario_SnapshotRestoreMidOrder`. |
| "Prove the machine has no dead ends or ambiguous steps." | **`analysis.Analyze`** returns no findings (no dead states, unreachable states, or nondeterminism). | `TestModel_AnalyzeClean`. |

## The recommended patterns, in one place

- **Context is a value, not a pointer.** `Order` is passed by value; the kernel and
  guards never alias it. The only writes happen inside `Assign` reducers, each a pure
  function from the prior `Order` (and the triggering event) to the next.
- **Results arrive as events, not side channels.** When the payment service or an
  actor completes, the host re-fires the routing event with the result as its payload;
  the `onDone` reducer reads it from `AssignCtx.Event`. There is no "last result"
  back-reference to thread through the builder.
- **Decision logic is data.** Guards are authored as expressions over a typed schema:
  Core for the common compare/membership cases (evaluated in-kernel, zero dependency),
  Rich (CEL) when you need real expressions, so tooling can read and check them.
- **The host owns the loop.** Crucible decides; the `Rig` dispatches the resulting
  effects (start a service, arm a timer, spawn an actor) to the world. The decision
  core stays pure.

## Running it

```
go test ./...            # scenario + example tests
go test -run Example -v  # the runnable Example* walkthroughs
```

See `scenario_test.go` for the happy path, the refund saga, the SLA breach, the
declined-authorization edge, and the snapshot/restore-across-restart flow.
