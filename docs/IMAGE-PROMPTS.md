# Crucible docs — image prompt manifest

Prompts for every illustration the docs site expects, written for **Gemini "Nano
Banana 2.0"** (natural-language, scene-based prompts — not comma-tag style).

**How to use:** generate each image at the stated aspect, export to the listed path
(replacing the labeled placeholder under `docs/src/assets/placeholders/` or the slot's
target), then remove the placeholder reference. Feed the **Shared style system** below as
context first (or prepend it to each prompt) so the set stays visually cohesive — most
important for the recurring **sky-squid mascot**, which must look like the same character
across every image. Unless a prompt explicitly asks for a word/label, instruct the model
to render **no text** (avoids garbled type).

---

## Shared style system (prepend to every prompt)

> Visual identity for "Crucible," a Go state-machine toolkit. Aesthetic: a **modern
> metalworking foundry** rendered as clean, semi-flat vector illustration with soft
> volumetric glow — technical but warm, never gritty or photoreal. **Palette:** deep
> steel/charcoal backgrounds (#0E1116, #1B1E24), molten **ember-orange** (#E8702A) and
> **amber** (#F0A04B) as the primary accent, **copper** (#B87333) for tooling and metal,
> cool steel-gray midtones (#8A94A6), and a luminous **cyan-teal** (#36D0C4) reserved for
> "state / data / logic" glow (statechart nodes, arcs, the IR). Lighting: dark scene lit
> by the glow of molten metal and the teal logic-light. Composition: generous negative
> space, clear focal subject, subtle depth; reads well small. **The Crucible sky-squid
> mascot** (when present): a friendly, characterful stylized squid with a rounded mantle,
> large expressive eyes, and several dexterous tentacles; copper-and-teal coloring with an
> ember underglow; often wears a small foundry apron and wields smith's tongs. Keep its
> design identical across images. No photorealism, no harsh horror, no clutter, no text
> unless requested.

---

## Brand & core assets

These are referenced from `astro.config.mjs` / the landing page and the scaffold ships
labeled placeholders for them under `docs/src/assets/placeholders/` (and `docs/public/`).

### logo — `docs/src/assets/placeholders/logo.svg` · ~200×48 (wide wordmark lockup)
> A compact horizontal logo lockup for "Crucible": a minimal emblem of a glowing crucible
> vessel pouring a single molten-teal droplet that forms a small state-node, set beside
> clean modern wordmark space. Emblem only is fine if wordmark is added in code. Ember/copper
> on transparent, crisp at small sizes, simple enough to read as a favicon.

### favicon — `docs/public/favicon.svg` · 1:1
> Just the Crucible emblem from the logo — a glowing crucible vessel with one molten-teal
> droplet/state-node — centered, bold, legible at 16px. Transparent background.

### hero — `docs/src/assets/placeholders/hero.svg` · 16:9 (landing splash)
> Wide hero: the Crucible sky-squid mascot at a foundry workbench, tongs in hand, lifting a
> luminous teal statechart (glowing nodes joined by directed arcs) out of a molten-ember
> crucible as if it were freshly forged metal. Warm ember glow below, cool teal logic-light
> above. Aspirational, welcoming, lots of headroom for overlaid title text in code.

### mascot — `docs/src/assets/placeholders/mascot-sky-squid.svg` · 1:1 (character portrait)
> Clean character portrait of the Crucible sky-squid mascot: rounded mantle, big friendly
> eyes, several tentacles, copper-and-teal coloring with an ember underglow, small foundry
> apron, holding smith's tongs. Neutral dark-steel background with soft glow. This is the
> canonical reference sheet — define the character clearly so other images can match it.

### social-card — `docs/src/assets/placeholders/social-card.svg` + `docs/public/social-card.svg` · 1200×630 (og:image)
> Open-Graph social card: on the left, the Crucible sky-squid forging a glowing teal
> statechart over a molten crucible; on the right, generous dark-steel space for the title
> "Crucible" and tagline "Forge event-driven services in Go" (you may render this exact text
> crisply, or leave the right third clear for text added later). Foundry palette, balanced.

---

## In-page illustrations

Each is referenced by an `IMAGE-SLOT: <slug>` marker on the listed page. Suggested target:
`docs/src/assets/<slug>.{webp,png}` (update the page's image reference when you add it).

### state-kernel-hero · 16:9 · start/introduction
> A glowing crucible casting a luminous teal statechart — nodes and directed arcs — as molten
> metal pours into a mold; the sky-squid mascot peers over the rim, watching the machine take
> shape. Conveys "forge an abstract definition into a running instance."

### two-front-ends-one-ir · 16:9 · concepts/ir-and-the-split
> A central glowing IR "ingot" (a JSON-etched metal bar) fed by two channels: on the left a
> code editor pouring molten Go, on the right a visual node-graph editor pouring molten diagram
> shapes; both crystallize into the *same* ingot, which then casts a running instance. The
> sky-squid inspects the shared ingot. Conveys "two front-ends, one IR."

### nested-rings · 16:9 · authoring/hierarchical-states
> The sky-squid cradling concentric glowing teal state-rings, an inner child-ring nested inside
> a larger "Active" super-ring, showing containment/hierarchy.

### orthogonal-lanes · 16:9 · authoring/parallel-regions
> The sky-squid using two tentacles to steer two parallel glowing lanes split by a dashed
> divider — a "kitchen" lane and an SLA-"timer" lane — running at once. Conveys orthogonal regions.

### gatekeeper · 3:2 · authoring/guards
> The sky-squid as a luminous gatekeeper holding up a glowing condition card, deciding whether to
> open a transition arch between two state-nodes. Conveys a guard gating a transition.

### effect-conveyor · 16:9 · authoring/actions-and-effects
> The sky-squid placing sealed glowing "effect" parcels onto a conveyor belt marked for the host
> to dispatch, while a still, calm kernel booth in the background emits but never touches them.
> Conveys "effects are data the host dispatches; the kernel does no IO."

### reducer-fold · 3:2 · authoring/assigns
> The sky-squid folding a glowing ribbon of "context" through a row of ordered stamping stations,
> each producing the next value of the ribbon. Conveys ordered, value-returning reducers.

### invoked-service · 16:9 · authoring/services
> The sky-squid dispatching a glowing courier-orb out to an external system, with a return thread
> carrying an "onDone" signal back home to the waiting state. Conveys an invoked async service.

### actors-supervision-tree · 16:9 · authoring/actors
> A foundry overseer (the sky-squid) routing molten message-sparks between glowing child crucibles
> — a parent crucible above, "kitchen" and "courier" child crucibles below. Conveys actors + messaging.

### history-resume · 16:9 · authoring/history
> A glowing foundry ledger showing a last-cast configuration, with an arrow looping a worker back to
> the exact mold they left earlier. Conveys history states resuming prior configuration.

### delayed-timer · 16:9 · authoring/delayed-transitions
> A foundry hourglass wired to a molten-edge switch, sand draining toward an "SLA-breach" spark that
> will trip a transition when time runs out. Conveys `after(d)` delayed transitions.

### snapshot-restore · 16:9 · authoring/snapshots-and-inspection
> A foundry pour freezing a glowing workpiece into a labeled ingot (snapshot), then the same ingot
> re-melting into an identical mold (restore). Conveys clean snapshot/restore via value semantics.

### assay-gate · 16:9 · authoring/assay
> A foundry inspector (the sky-squid) assaying an incoming ingot against a glowing requirement-template
> at the gate, accepting a sound casting and rejecting a flawed one. Conveys `Assay` validating an entity.

### ir-interchange · 16:9 · serialization/overview (or json-ir)
> A glowing statechart ingot being poured into a JSON mold and then re-cast back into a running machine;
> the sky-squid inspects both forms and finds them identical. Conveys lossless JSON round-trip.

### render-twins · 16:9 · serialization/visualization
> The sky-squid holding up two identical glowing statechart prints — one forged from code, one from a
> JSON document — overlaid to reveal a perfect match. Conveys "same graph renders from code or JSON."

### order-saga · 16:9 · examples/overview
> The sky-squid conducting a food-delivery order through a glowing statechart: a payment hold, parallel
> "kitchen" and "courier" lanes running under a draining SLA timer, branching to "delivered" or a "refund"
> compensation arc. The flagship example scene — rich but readable.

### analysis-toolbox · 16:9 · analysis/overview
> A foundry inspection bench laying out five glowing instruments arranged around a translucent statechart
> ingot, the sky-squid examining it under a loupe. Conveys "the machine is data you can measure."

### temper-pass · 16:9 · analysis/static-analysis
> A glowing statechart casting passing under a diagnostic scanner that lights orphaned nodes amber and
> severed edges red, positioned just before a quench tank. Conveys non-failing lint diagnostics before freeze.

### witness-path · 16:9 · analysis/verification
> A glowing statechart with one luminous route traced end-to-end while the rest dims, a foundry ledger
> beside it annotating the event sequence. Conveys a verification witness/counterexample path.

### disjoint-guards · 3:2 · analysis/symbolic-guards
> Two molten transition arcs leaving a single node; the sky-squid holds up a glowing proof-token showing the
> two guard conditions can never both be true, while a third arc is grayed and marked "unknown." Conveys
> provable disjointness (and conservative unknowns).

### conformance-replay · 16:9 · analysis/conformance
> A foundry quality bench replaying a stamped "golden" casting against a freshly produced one, a glowing
> diff-meter reading "match," the sky-squid signing off. Conveys golden-scenario conformance.

### evolution-diff · 16:9 · analysis/evolution
> Two glowing statechart castings side by side with changed nodes haloed; a foundry stamp reads "MINOR" over
> the safe one and "MAJOR" over the breaking one. (You may render those two words crisply.) Conveys SemVer diffing.

### decision-core-seam · 16:9 · integrating/overview
> A glowing crucible "decision core" at center emitting effect-sparks across a clean seam to a host, which
> routes them onward to a broker, a store, and an RPC; the sky-squid tends the seam. Conveys "pure core, host
> dispatches effects, one machine many consumers."

### project-and-apply · 16:9 · integrating/pointer-heavy-codebases
> A heavy "pointer-aggregate" ingot casting off a slim glowing "value-projection" wafer into the crucible; the
> crucible returns effect-sparks that the host stamps back onto the heavy ingot inside a glowing transaction ring.
> Conveys the value-projection adoption recipe for mutation-heavy codebases.
