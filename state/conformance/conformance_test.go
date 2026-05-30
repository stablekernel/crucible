package conformance_test

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
)

// update regenerates the committed golden scenario fixtures when set, so a
// deliberate machine change can refresh the goldens in one reviewable diff.
var update = flag.Bool("update", false, "regenerate golden scenario fixtures")

// A neutral document-approval machine, self-contained so the conformance
// package's tests carry no dependency on the kernel's own example fixtures.

type docState int

const (
	draft docState = iota
	submitted
	approved
	published
	archived
)

func (s docState) String() string {
	switch s {
	case draft:
		return "Draft"
	case submitted:
		return "Submitted"
	case approved:
		return "Approved"
	case published:
		return "Published"
	case archived:
		return "Archived"
	default:
		return "Draft?"
	}
}

type docEvent int

const (
	submit docEvent = iota
	approve
	requestChanges
	publish
	archive
)

func (e docEvent) String() string {
	switch e {
	case submit:
		return "Submit"
	case approve:
		return "Approve"
	case requestChanges:
		return "RequestChanges"
	case publish:
		return "Publish"
	case archive:
		return "Archive"
	default:
		return "Submit?"
	}
}

type document struct {
	status     docState
	reviewerID *string
}

func emit(ctx state.ActionCtx[*document]) (state.Effect, error) {
	name, _ := ctx.Params["event"].(string)
	return emitted{name: name}, nil
}

type emitted struct{ name string }

func docRegistry() *state.Registry[*document] {
	return state.NewRegistry[*document]().
		Guard("hasReviewer", func(ctx state.GuardCtx[*document]) bool {
			return ctx.Entity.reviewerID != nil
		}).
		Action("emit", emit)
}

func buildDocMachine() *state.Machine[docState, docEvent, *document] {
	return state.Forge[docState, docEvent, *document]("document").
		Guard("hasReviewer", func(ctx state.GuardCtx[*document]) bool {
			return ctx.Entity.reviewerID != nil
		}).
		Action("emit", emit).
		State(draft).
		State(submitted).
		State(approved).
		State(published).
		State(archived).
		Initial(draft).
		CurrentStateFn(func(d *document) docState { return d.status }).
		Transition(draft).On(submit).GoTo(submitted).Do("emit", state.P{"event": "submitted"}).
		Transition(submitted).On(approve).GoTo(approved).When("hasReviewer").Do("emit", state.P{"event": "approved"}).
		Transition(submitted).On(requestChanges).GoTo(draft).
		Transition(approved).On(publish).GoTo(published).Do("emit", state.P{"event": "published"}).
		Transition(draft).On(archive).GoTo(archived).
		Transition(submitted).On(archive).GoTo(archived).
		Quench()
}

func docCodec() conformance.EventCodec[docEvent] {
	names := map[string]docEvent{
		"Submit": submit, "Approve": approve, "RequestChanges": requestChanges,
		"Publish": publish, "Archive": archive,
	}
	return conformance.EventCodec[docEvent]{
		Named: func(e docEvent) string { return e.String() },
		Resolve: func(name string) (docEvent, bool) {
			e, ok := names[name]
			return e, ok
		},
	}
}

func newDoc() *document { return &document{status: draft, reviewerID: strptr("rev-1")} }

func strptr(s string) *string { return &s }

func TestGenerateScenarios_ReachesEveryState(t *testing.T) {
	m := buildDocMachine()
	scs, err := conformance.GenerateScenarios(m, func(e docEvent) string { return e.String() })
	if err != nil {
		t.Fatalf("GenerateScenarios: %v", err)
	}
	// Every non-initial reachable state gets a scenario: Submitted, Approved,
	// Published, Archived, plus Draft is reachable again via RequestChanges but
	// BFS marks it visited from the initial — so four targets.
	want := map[string]bool{"Submitted": true, "Approved": true, "Published": true, "Archived": true}
	got := map[string]bool{}
	for _, sc := range scs {
		last := sc.Assertions[0].Expected
		got[fmt.Sprint(last)] = true
	}
	for w := range want {
		if !got[w] {
			t.Errorf("no scenario reaches %q (got %v)", w, got)
		}
	}
}

func TestRunAgainst_GeneratedScenariosPass(t *testing.T) {
	m := buildDocMachine()
	scs, err := conformance.GenerateScenarios(m, func(e docEvent) string { return e.String() })
	if err != nil {
		t.Fatalf("GenerateScenarios: %v", err)
	}
	for _, sc := range scs {
		res := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft)
		if !res.Passed() {
			t.Errorf("scenario %q did not pass: %+v", sc.Name, res.Assertions)
		}
	}
}

func TestCompareMachines_IdenticalMachinesConform(t *testing.T) {
	ref := buildDocMachine()
	sub := buildDocMachine()
	scs, _ := conformance.GenerateScenarios(ref, func(e docEvent) string { return e.String() })
	if err := conformance.CompareMachines(ref, sub, scs, docCodec(), draft, func() *document { return newDoc() }); err != nil {
		t.Fatalf("identical machines should conform: %v", err)
	}
}

func TestCompareMachines_DivergenceIsReported(t *testing.T) {
	ref := buildDocMachine()
	// A subject that omits the publish effect diverges on effects.
	sub := state.Forge[docState, docEvent, *document]("document").
		Action("emit", emit).
		State(draft).State(submitted).State(approved).State(published).State(archived).
		Initial(draft).
		CurrentStateFn(func(d *document) docState { return d.status }).
		Transition(draft).On(submit).GoTo(submitted).Do("emit", state.P{"event": "submitted"}).
		Transition(approved).On(publish).GoTo(published). // no effect
		Quench()

	scs := []conformance.Scenario{
		{
			MachineID:    "document",
			Name:         "publish-flow",
			InitialState: "Approved",
			Events:       []conformance.Event{{Event: "Publish"}},
		},
	}
	err := conformance.CompareMachines(ref, sub, scs, docCodec(), approved, func() *document { return &document{status: approved} })
	if err == nil {
		t.Fatal("expected divergence on effects")
	}
	var ce *conformance.ErrConformance
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want *ErrConformance", err)
	}
}

func TestRoundTripIdentity_HoldsForDocMachine(t *testing.T) {
	m := buildDocMachine()
	scs, _ := conformance.GenerateScenarios(m, func(e docEvent) string { return e.String() })
	if err := conformance.RoundTripIdentity(m, docRegistry(), scs, docCodec(), draft, func() *document { return newDoc() }); err != nil {
		t.Fatalf("round-trip identity failed: %v", err)
	}
}

// TestGoldenScenarios replays the committed scenario fixtures and asserts each
// passes — the golden-scenarios pillar. A change to the machine that breaks a
// committed scenario is a visible CI failure.
func TestGoldenScenarios(t *testing.T) {
	m := buildDocMachine()
	dir := filepath.Join("testdata", "scenarios")

	if *update {
		scs, err := conformance.GenerateScenarios(m, func(e docEvent) string { return e.String() })
		if err != nil {
			t.Fatalf("generate for update: %v", err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		for _, sc := range scs {
			data, err := json.MarshalIndent(sc, "", "  ")
			if err != nil {
				t.Fatalf("marshal %s: %v", sc.Name, err)
			}
			data = append(data, '\n')
			if err := os.WriteFile(filepath.Join(dir, sc.Name+".json"), data, 0o644); err != nil {
				t.Fatalf("write %s: %v", sc.Name, err)
			}
		}
		t.Logf("wrote %d golden scenarios to %s", len(scs), dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read golden dir: %v", err)
	}
	seen := 0
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		seen++
		t.Run(e.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatalf("read %s: %v", e.Name(), err)
			}
			sc, err := conformance.LoadScenario(data)
			if err != nil {
				t.Fatalf("load %s: %v", e.Name(), err)
			}
			start := draft
			switch sc.InitialState {
			case "Submitted":
				start = submitted
			case "Approved":
				start = approved
			}
			res := conformance.RunAgainst(m, sc, newDoc(), docCodec(), start)
			if !res.Passed() {
				t.Errorf("golden %s failed: %+v", e.Name(), res.Assertions)
			}
		})
	}
	if seen == 0 {
		t.Fatal("no golden scenarios found under testdata/scenarios")
	}
}

func TestLoadScenario_RejectsBadSchema(t *testing.T) {
	_, err := conformance.LoadScenario([]byte(`{"schemaVersion": 99, "machineId": "x"}`))
	var sv *conformance.ErrSchemaVersion
	if !errors.As(err, &sv) {
		t.Fatalf("err = %v, want *ErrSchemaVersion", err)
	}
}

func TestRunAgainst_UnknownEvent(t *testing.T) {
	m := buildDocMachine()
	sc := conformance.Scenario{
		MachineID: "document", InitialState: "Draft",
		Events: []conformance.Event{{Event: "Nope"}},
	}
	res := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft)
	var ue *conformance.ErrUnknownEvent
	if !errors.As(res.Err, &ue) {
		t.Fatalf("err = %v, want *ErrUnknownEvent", res.Err)
	}
}
