package evolution_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/evolution"
)

// Neutral example machines, built as IR literals so the tests exercise the
// differ directly without coupling to the DSL or a host registry.

// docMachine: draft -> submitted -> approved -> published (a flat document
// workflow). The approval transition carries a guard.
func docMachine() *state.IR[string, string, any] {
	return &state.IR[string, string, any]{
		Name:       "document",
		Initial:    "draft",
		HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "draft", Transitions: []state.Transition[string, string, any]{
				{From: "draft", On: "submit", To: "submitted"},
			}},
			{Name: "submitted", Transitions: []state.Transition[string, string, any]{
				{From: "submitted", On: "approve", To: "approved", Guards: []state.Ref{{Name: "hasReviewer"}}},
			}},
			{Name: "approved", Transitions: []state.Transition[string, string, any]{
				{From: "approved", On: "publish", To: "published", Effects: []state.Ref{{Name: "notify"}}},
			}},
			{Name: "published", IsFinal: true},
		},
	}
}

// jobMachine: queued -> running -> {succeeded, failed}.
func jobMachine() *state.IR[string, string, any] {
	return &state.IR[string, string, any]{
		Name:       "job",
		Initial:    "queued",
		HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "queued", Transitions: []state.Transition[string, string, any]{
				{From: "queued", On: "start", To: "running"},
			}},
			{Name: "running", Transitions: []state.Transition[string, string, any]{
				{From: "running", On: "succeed", To: "succeeded"},
				{From: "running", On: "fail", To: "failed"},
			}},
			{Name: "succeeded", IsFinal: true},
			{Name: "failed", IsFinal: true},
		},
	}
}

// mediaPlayer: a compound state with nested children (stopped/playing/paused).
func mediaPlayer() *state.IR[string, string, any] {
	playing := "playing"
	return &state.IR[string, string, any]{
		Name:       "media-player",
		Initial:    "active",
		HasInitial: true,
		States: []state.State[string, string, any]{
			{
				Name:         "active",
				InitialChild: &playing,
				Children: []state.State[string, string, any]{
					{Name: "playing", Transitions: []state.Transition[string, string, any]{
						{From: "playing", On: "pause", To: "paused"},
					}},
					{Name: "paused", Transitions: []state.Transition[string, string, any]{
						{From: "paused", On: "resume", To: "playing"},
					}},
				},
			},
			{Name: "stopped", IsFinal: true},
		},
	}
}

func TestDiff_Identical(t *testing.T) {
	r := evolution.Diff(docMachine(), docMachine())
	if !r.Empty() {
		t.Fatalf("identical machines should produce an empty report, got:\n%s", r)
	}
	if r.Breaking() {
		t.Fatal("identical machines must not be breaking")
	}
	if got := r.SemverBump(); got != evolution.Patch {
		t.Fatalf("identical -> Patch, got %q", got)
	}
	if r.String() != "no changes" {
		t.Fatalf("empty report String() = %q", r.String())
	}
}

func TestDiff_AddState_Additive(t *testing.T) {
	old := docMachine()
	updated := docMachine()
	updated.States = append(updated.States, state.State[string, string, any]{Name: "archived", IsFinal: true})
	// Wire a transition into it from an existing state.
	updated.States[3].Transitions = append(updated.States[3].Transitions,
		state.Transition[string, string, any]{From: "published", On: "archive", To: "archived"})

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("adding a state + transition is additive, got breaking:\n%s", r)
	}
	if got := r.SemverBump(); got != evolution.Minor {
		t.Fatalf("additive -> Minor, got %q", got)
	}
	assertKinds(t, r, evolution.KindStateAdded, evolution.KindTransitionAdded)
}

func TestDiff_RemoveState_Breaking(t *testing.T) {
	old := docMachine()
	updated := docMachine()
	updated.States = updated.States[:3] // drop "published"

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatal("removing a state must be breaking")
	}
	if got := r.SemverBump(); got != evolution.Major {
		t.Fatalf("breaking -> Major, got %q", got)
	}
	if !hasKind(r, evolution.KindStateRemoved) {
		t.Fatalf("expected a state_removed change, got:\n%s", r)
	}
}

func TestDiff_Retarget_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	// running/succeed now points at "failed" instead of "succeeded".
	updated.States[1].Transitions[0].To = "failed"

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatal("retargeting a transition must be breaking")
	}
	if !hasKind(r, evolution.KindTransitionRetargeted) {
		t.Fatalf("expected transition_retargeted, got:\n%s", r)
	}
}

func TestDiff_AddTransition_Additive(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[0].Transitions = append(updated.States[0].Transitions,
		state.Transition[string, string, any]{From: "queued", On: "cancel", To: "failed"})

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("adding a transition is additive, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindTransitionAdded) {
		t.Fatalf("expected transition_added, got:\n%s", r)
	}
}

func TestDiff_RemoveTransition_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[1].Transitions = updated.States[1].Transitions[:1] // drop "fail"

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatal("removing a transition must be breaking")
	}
	if !hasKind(r, evolution.KindTransitionRemoved) {
		t.Fatalf("expected transition_removed, got:\n%s", r)
	}
}

func TestDiff_InitialChanged_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.Initial = "running"

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatal("changing the initial state must be breaking")
	}
	if !hasKind(r, evolution.KindInitialChanged) {
		t.Fatalf("expected initial_changed, got:\n%s", r)
	}
}

func TestDiff_MachineRenamed_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.Name = "task"

	r := evolution.Diff(old, updated)
	if !r.Breaking() || !hasKind(r, evolution.KindMachineRenamed) {
		t.Fatalf("renaming the machine must be breaking, got:\n%s", r)
	}
}

func TestDiff_AddGuard_FlaggedAdditive(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[0].Transitions[0].Guards = []state.Ref{{Name: "quotaOK"}}

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("adding a guard is additive per the guide, got breaking:\n%s", r)
	}
	if !hasKind(r, evolution.KindGuardAdded) {
		t.Fatalf("expected guard_added, got:\n%s", r)
	}
	// It must be flagged so a reviewer audits the data.
	if !strings.Contains(r.String(), "FLAGGED") {
		t.Fatalf("guard addition must be flagged for a data audit, got:\n%s", r)
	}
}

func TestDiff_RemoveGuard_Additive(t *testing.T) {
	old := docMachine()
	updated := docMachine()
	updated.States[1].Transitions[0].Guards = nil // drop hasReviewer

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("removing a guard is a loosening (additive), got:\n%s", r)
	}
	if !hasKind(r, evolution.KindGuardRemoved) {
		t.Fatalf("expected guard_removed, got:\n%s", r)
	}
}

// guardedBranchMachine: a single state "router" routing event "go" to one of two
// targets depending on a guard. This is the canonical "same event, different
// guard -> different target" pattern; the two branches share (From, On) but are
// distinct transitions.
func guardedBranchMachine() *state.IR[string, string, any] {
	return &state.IR[string, string, any]{
		Name:       "router",
		Initial:    "router",
		HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "router", Transitions: []state.Transition[string, string, any]{
				{From: "router", On: "go", To: "fast", Guards: []state.Ref{{Name: "isPriority"}}},
				{From: "router", On: "go", To: "slow", Guards: []state.Ref{{Name: "isStandard"}}},
			}},
			{Name: "fast", IsFinal: true},
			{Name: "slow", IsFinal: true},
		},
	}
}

// TestDiff_GuardedBranches_RemovedBranchIsBreaking proves that removing one of two
// guarded branches sharing (From, On) is reported as a breaking transition
// removal. Keying transitions only by (From, On) would collapse the two branches
// into one slot and silently hide the removal.
func TestDiff_GuardedBranches_RemovedBranchIsBreaking(t *testing.T) {
	old := guardedBranchMachine()
	updated := guardedBranchMachine()
	// Drop the isStandard -> slow branch entirely.
	updated.States[0].Transitions = updated.States[0].Transitions[:1]

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("removing a guarded branch must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindTransitionRemoved) {
		t.Fatalf("expected transition_removed for the dropped branch, got:\n%s", r)
	}
}

// TestDiff_GuardedBranches_RetargetOneBranchIsBreaking proves that retargeting a
// single guarded branch (leaving its sibling untouched) surfaces a breaking
// retarget. The (From, On)-only key kept whichever branch the map saw last, so a
// retarget of the other branch was invisible.
func TestDiff_GuardedBranches_RetargetOneBranchIsBreaking(t *testing.T) {
	old := guardedBranchMachine()
	updated := guardedBranchMachine()
	// Retarget the isStandard branch from slow to fast.
	updated.States[0].Transitions[1].To = "fast"

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("retargeting a guarded branch must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindTransitionRetargeted) {
		t.Fatalf("expected transition_retargeted for the changed branch, got:\n%s", r)
	}
}

// TestDiff_GuardedBranches_AddBranchIsAdditive proves that adding a third guarded
// branch on an existing (From, On) is additive, not a spurious breaking change.
func TestDiff_GuardedBranches_AddBranchIsAdditive(t *testing.T) {
	old := guardedBranchMachine()
	updated := guardedBranchMachine()
	updated.States = append(updated.States, state.State[string, string, any]{Name: "express", IsFinal: true})
	updated.States[0].Transitions = append(updated.States[0].Transitions,
		state.Transition[string, string, any]{From: "router", On: "go", To: "express", Guards: []state.Ref{{Name: "isExpress"}}})

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("adding a guarded branch must be additive, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindTransitionAdded) {
		t.Fatalf("expected transition_added for the new branch, got:\n%s", r)
	}
}

func TestDiff_EffectAndRemoval(t *testing.T) {
	old := docMachine()
	updated := docMachine()
	updated.States[0].Transitions[0].Effects = []state.Ref{{Name: "audit"}} // add effect
	updated.States[2].Transitions[0].Effects = nil                          // remove "notify"

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("effect add/remove is additive, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindEffectAdded) || !hasKind(r, evolution.KindEffectRemoved) {
		t.Fatalf("expected effect_added and effect_removed, got:\n%s", r)
	}
}

func TestDiff_MetadataAndWaitMode_Additive(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[0].OwnedBy = "platform-team"
	updated.States[0].Transitions[0].WaitMode = state.FireAndForget

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("OwnedBy + WaitMode changes are additive, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindMetadataChanged) || !hasKind(r, evolution.KindWaitModeChanged) {
		t.Fatalf("expected metadata_changed and waitmode_changed, got:\n%s", r)
	}
}

func TestDiff_FinalChanged_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[2].IsFinal = false

	r := evolution.Diff(old, updated)
	if !r.Breaking() || !hasKind(r, evolution.KindFinalChanged) {
		t.Fatalf("toggling IsFinal must be breaking, got:\n%s", r)
	}
}

func TestDiff_NestedChild_Breaking(t *testing.T) {
	old := mediaPlayer()
	updated := mediaPlayer()
	// Remove the nested "paused" child state.
	updated.States[0].Children = updated.States[0].Children[:1]

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatal("removing a nested child state must be breaking")
	}
	// The path must reflect the hierarchy.
	if !strings.Contains(r.String(), "active.paused") {
		t.Fatalf("expected a dotted nested path active.paused, got:\n%s", r)
	}
}

func TestDiff_NestedChildAdded_Additive(t *testing.T) {
	old := mediaPlayer()
	updated := mediaPlayer()
	updated.States[0].Children = append(updated.States[0].Children,
		state.State[string, string, any]{Name: "buffering"})

	r := evolution.Diff(old, updated)
	if r.Breaking() {
		t.Fatalf("adding a nested child state is additive, got:\n%s", r)
	}
	if !strings.Contains(r.String(), "active.buffering") {
		t.Fatalf("expected nested path active.buffering, got:\n%s", r)
	}
}

// TestDiffMachines_AgreesWithDiff drives the Quenched-machine entry point and
// asserts it classifies a breaking change identically to Diff over the same IRs.
func TestDiffMachines_AgreesWithDiff(t *testing.T) {
	oldM := state.Forge[string, string, any]("doc").
		State("draft").
		Transition("draft").On("submit").GoTo("review").
		State("review").
		Transition("review").On("approve").GoTo("done").
		State("done").Final().
		Initial("draft").
		Quench()
	// The updated machine drops the review->done transition: a breaking removal.
	newM := state.Forge[string, string, any]("doc").
		State("draft").
		Transition("draft").On("submit").GoTo("review").
		State("review").
		State("done").Final().
		Initial("draft").
		Quench()

	r, err := evolution.DiffMachines(oldM, newM)
	if err != nil {
		t.Fatalf("DiffMachines: %v", err)
	}
	if !r.Breaking() {
		t.Fatalf("DiffMachines should report the removal as breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindTransitionRemoved) {
		t.Fatalf("expected transition_removed, got:\n%s", r)
	}
}

// TestDiffMachines_IdenticalIsEmpty asserts DiffMachines over the same definition
// reports no change.
func TestDiffMachines_IdenticalIsEmpty(t *testing.T) {
	build := func() *state.Machine[string, string, any] {
		return state.Forge[string, string, any]("doc").
			State("draft").
			Transition("draft").On("submit").GoTo("done").
			State("done").Final().
			Initial("draft").
			Quench()
	}
	r, err := evolution.DiffMachines(build(), build())
	if err != nil {
		t.Fatalf("DiffMachines: %v", err)
	}
	if !r.Empty() {
		t.Fatalf("identical machines should diff empty, got:\n%s", r)
	}
}

// TestEvolutionErrorTypes_FormatAndUnwrap covers the Error and Unwrap methods of
// SerializeError and DecodeError directly, since a Machine that fails to serialize
// cannot be produced through the normal Forge/Quench path.
func TestEvolutionErrorTypes_FormatAndUnwrap(t *testing.T) {
	cause := errors.New("boom")

	se := &evolution.SerializeError{Side: "old", Err: cause}
	if !strings.Contains(se.Error(), "serialize old machine") {
		t.Fatalf("SerializeError.Error() = %q", se.Error())
	}
	if !errors.Is(se, cause) {
		t.Fatal("SerializeError should unwrap to its cause")
	}

	de := &evolution.DecodeError{Side: "new", Err: cause}
	if !strings.Contains(de.Error(), "decode new machine") {
		t.Fatalf("DecodeError.Error() = %q", de.Error())
	}
	if !errors.Is(de, cause) {
		t.Fatal("DecodeError should unwrap to its cause")
	}
}

func TestDiffJSON_RoundTrip(t *testing.T) {
	old := docMachine()
	updated := docMachine()
	updated.States = updated.States[:3] // breaking removal

	ob, nb := mustJSON(t, old), mustJSON(t, updated)
	r, err := evolution.DiffJSON[string, string, any](ob, nb)
	if err != nil {
		t.Fatalf("DiffJSON: %v", err)
	}
	if !r.Breaking() {
		t.Fatalf("DiffJSON should agree with Diff (breaking), got:\n%s", r)
	}
}

func TestDiffJSON_DecodeError(t *testing.T) {
	good := mustJSON(t, docMachine())
	_, err := evolution.DiffJSON[string, string, any]([]byte("{not json"), good)
	var de *evolution.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("expected *DecodeError, got %v", err)
	}
	if de.Side != "old" {
		t.Fatalf("decode error side = %q, want old", de.Side)
	}
	if errors.Unwrap(err) == nil {
		t.Fatal("DecodeError should wrap the underlying error")
	}

	_, err = evolution.DiffJSON[string, string, any](good, []byte("]["))
	if !errors.As(err, &de) || de.Side != "new" {
		t.Fatalf("expected new-side decode error, got %v", err)
	}
}

func TestRegionDiff(t *testing.T) {
	region := func(extra bool) *state.IR[string, string, any] {
		states := []state.State[string, string, any]{
			{Name: "a", Transitions: []state.Transition[string, string, any]{{From: "a", On: "x", To: "b"}}},
			{Name: "b"},
		}
		if extra {
			states = append(states, state.State[string, string, any]{Name: "c"})
		}
		return &state.IR[string, string, any]{
			Name:       "parallel",
			Initial:    "root",
			HasInitial: true,
			States: []state.State[string, string, any]{
				{Name: "root", Regions: []state.Region[string, string, any]{
					{Name: "r1", States: states},
				}},
			},
		}
	}

	// Add a state inside a region -> additive.
	r := evolution.Diff(region(false), region(true))
	if r.Breaking() {
		t.Fatalf("adding a state in a region is additive, got:\n%s", r)
	}
	if !strings.Contains(r.String(), "region:r1") {
		t.Fatalf("expected region path, got:\n%s", r)
	}

	// Remove the whole region -> breaking.
	withRegion := region(false)
	noRegion := region(false)
	noRegion.States[0].Regions = nil
	r = evolution.Diff(withRegion, noRegion)
	if !r.Breaking() {
		t.Fatalf("removing a region must be breaking, got:\n%s", r)
	}
}

// --- v1-freeze fidelity fixes (edit classes 1-7) ---

// TestDiff_TransitionReordered_Breaking proves that reordering a state's
// transition list (same multiset, different order) is breaking: order decides
// which transition fires first.
func TestDiff_TransitionReordered_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	// running has [succeed, fail]; swap to [fail, succeed].
	tr := updated.States[1].Transitions
	updated.States[1].Transitions = []state.Transition[string, string, any]{tr[1], tr[0]}

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("reordering transitions must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindTransitionReordered) {
		t.Fatalf("expected transition_reordered, got:\n%s", r)
	}
	if got := r.SemverBump(); got != evolution.Major {
		t.Fatalf("reorder -> Major, got %q", got)
	}
}

// TestDiff_TransitionReordered_NoFalsePositiveOnAdd proves the reorder check does
// not fire when a transition is genuinely added (a different multiset).
func TestDiff_TransitionReordered_NoFalsePositiveOnAdd(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[0].Transitions = append(updated.States[0].Transitions,
		state.Transition[string, string, any]{From: "queued", On: "cancel", To: "failed"})

	r := evolution.Diff(old, updated)
	if hasKind(r, evolution.KindTransitionReordered) {
		t.Fatalf("adding a transition must not be reported as a reorder, got:\n%s", r)
	}
}

// TestDiff_GuardStructureChanged_Breaking proves that changing a composite
// guard's operator structure (And -> Or) with an identical leaf set is breaking.
func TestDiff_GuardStructureChanged_Breaking(t *testing.T) {
	build := func(expr state.GuardNode[string]) *state.IR[string, string, any] {
		e := expr
		return &state.IR[string, string, any]{
			Name: "router", Initial: "router", HasInitial: true,
			States: []state.State[string, string, any]{
				{Name: "router", Transitions: []state.Transition[string, string, any]{
					{From: "router", On: "go", To: "done", GuardExpr: &e},
				}},
				{Name: "done", IsFinal: true},
			},
		}
	}
	old := build(state.And(state.Guard[string]("a"), state.Guard[string]("b")))
	updated := build(state.Or(state.Guard[string]("a"), state.Guard[string]("b")))

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("changing guard operator structure must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindGuardStructureChanged) {
		t.Fatalf("expected guard_structure_changed, got:\n%s", r)
	}
	// The leaf set is identical, so no guard add/remove must fire.
	if hasKind(r, evolution.KindGuardAdded) || hasKind(r, evolution.KindGuardRemoved) {
		t.Fatalf("identical leaf set must not register as add/remove, got:\n%s", r)
	}
}

// TestDiff_InitialChildChanged_Breaking proves that flipping a compound state's
// InitialChild (default descent) is breaking.
func TestDiff_InitialChildChanged_Breaking(t *testing.T) {
	old := mediaPlayer()
	updated := mediaPlayer()
	paused := "paused"
	updated.States[0].InitialChild = &paused

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("changing InitialChild must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindInitialChildChanged) {
		t.Fatalf("expected initial_child_changed, got:\n%s", r)
	}
}

// TestDiff_RegionInitialChildChanged_Breaking proves a Region.InitialChild flip
// is breaking.
func TestDiff_RegionInitialChildChanged_Breaking(t *testing.T) {
	build := func(child string) *state.IR[string, string, any] {
		c := child
		return &state.IR[string, string, any]{
			Name: "parallel", Initial: "root", HasInitial: true,
			States: []state.State[string, string, any]{
				{Name: "root", Regions: []state.Region[string, string, any]{
					{Name: "r1", InitialChild: &c, States: []state.State[string, string, any]{
						{Name: "a"}, {Name: "b"},
					}},
				}},
			},
		}
	}
	r := evolution.Diff(build("a"), build("b"))
	if !r.Breaking() {
		t.Fatalf("changing region InitialChild must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindInitialChildChanged) {
		t.Fatalf("expected initial_child_changed, got:\n%s", r)
	}
}

// TestDiff_HistoryChanged_Breaking proves that flipping a state's HistoryType is
// breaking.
func TestDiff_HistoryChanged_Breaking(t *testing.T) {
	old := mediaPlayer()
	updated := mediaPlayer()
	updated.States[0].HistoryType = state.HistoryDeep

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("changing HistoryType must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindHistoryChanged) {
		t.Fatalf("expected history_changed, got:\n%s", r)
	}
}

// TestDiff_ContextSchemaChanged_Breaking proves that adding/retyping a context
// schema field is breaking.
func TestDiff_ContextSchemaChanged_Breaking(t *testing.T) {
	withCtx := func(fields ...state.SchemaField) *state.IR[string, string, any] {
		ir := jobMachine()
		ir.Context = &state.ContextSchema{Fields: fields}
		return ir
	}
	old := withCtx(state.SchemaField{Name: "retries", Kind: "int"})
	updated := withCtx(
		state.SchemaField{Name: "retries", Kind: "int"},
		state.SchemaField{Name: "owner", Kind: "string"},
	)

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("adding a context field must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindContextSchemaChanged) {
		t.Fatalf("expected context_schema_changed, got:\n%s", r)
	}

	// Retype an existing field.
	retyped := withCtx(state.SchemaField{Name: "retries", Kind: "string"})
	r = evolution.Diff(old, retyped)
	if !r.Breaking() || !hasKind(r, evolution.KindContextSchemaChanged) {
		t.Fatalf("retyping a context field must be breaking, got:\n%s", r)
	}

	// nil -> set is breaking.
	r = evolution.Diff(jobMachine(), old)
	if !r.Breaking() || !hasKind(r, evolution.KindContextSchemaChanged) {
		t.Fatalf("introducing a context schema must be breaking, got:\n%s", r)
	}

	// Identical schemas produce no schema change.
	r = evolution.Diff(old, withCtx(state.SchemaField{Name: "retries", Kind: "int"}))
	if hasKind(r, evolution.KindContextSchemaChanged) {
		t.Fatalf("identical context schema must not register a change, got:\n%s", r)
	}
}

// TestDiff_EventlessAdded_Breaking proves that adding an eventless (Always) edge
// is breaking, not an additive transition_added.
func TestDiff_EventlessAdded_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[0].Transitions = append(updated.States[0].Transitions,
		state.Transition[string, string, any]{From: "queued", On: "", To: "running", EventLess: true})

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("adding an eventless edge must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindEventlessChanged) {
		t.Fatalf("expected eventless_changed, got:\n%s", r)
	}
}

// TestDiff_EventlessFlipped_Breaking proves that flipping an existing transition's
// EventLess flag is breaking.
func TestDiff_EventlessFlipped_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[0].Transitions[0].EventLess = true

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("flipping EventLess must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindEventlessChanged) {
		t.Fatalf("expected eventless_changed, got:\n%s", r)
	}
}

// TestDiff_UnknownStructuralDelta_Breaking proves the catch-all backstop: a field
// the differ has no specific rule for (here Forbidden) still forces a Major bump
// so future IR additions fail safe.
func TestDiff_UnknownStructuralDelta_Breaking(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[1].Transitions[0].Forbidden = true

	r := evolution.Diff(old, updated)
	if !r.Breaking() {
		t.Fatalf("an unmodeled structural field change must be breaking, got:\n%s", r)
	}
	if !hasKind(r, evolution.KindUnknownStructuralDelta) {
		t.Fatalf("expected unknown_structural_delta, got:\n%s", r)
	}
}

// TestDiff_UnknownStructuralDelta_NoDoubleFire proves the backstop does NOT fire
// for a difference already specifically reported (a retarget).
func TestDiff_UnknownStructuralDelta_NoDoubleFire(t *testing.T) {
	old := jobMachine()
	updated := jobMachine()
	updated.States[1].Transitions[0].To = "failed" // a modeled retarget

	r := evolution.Diff(old, updated)
	if hasKind(r, evolution.KindUnknownStructuralDelta) {
		t.Fatalf("a modeled difference must not trip the backstop, got:\n%s", r)
	}
}

// --- assertion helpers ---

func hasKind(r evolution.Report, k evolution.ChangeKind) bool {
	for _, c := range r.Changes {
		if c.Kind == k {
			return true
		}
	}
	return false
}

func assertKinds(t *testing.T, r evolution.Report, kinds ...evolution.ChangeKind) {
	t.Helper()
	for _, k := range kinds {
		if !hasKind(r, k) {
			t.Fatalf("expected change kind %q in report:\n%s", k, r)
		}
	}
}

func mustJSON(t *testing.T, ir *state.IR[string, string, any]) []byte {
	t.Helper()
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal IR: %v", err)
	}
	return b
}
