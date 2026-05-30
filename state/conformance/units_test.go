package conformance_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
)

func TestScenario_MarshalPinsSchemaVersion(t *testing.T) {
	sc := conformance.Scenario{MachineID: "m", InitialState: "A"}
	data, err := json.Marshal(sc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"schemaVersion":1`) {
		t.Fatalf("missing pinned schema version: %s", data)
	}
}

func TestTrace_MarshalPinsSchemaVersion(t *testing.T) {
	tr := conformance.Trace{MachineID: "m", FromState: "A", ToState: "B"}
	data, err := json.Marshal(tr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"schemaVersion":1`) {
		t.Fatalf("missing pinned schema version: %s", data)
	}
}

func TestErrorStrings(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&conformance.ErrSchemaVersion{Got: 9, Want: 1}, "unsupported schema version 9"},
		{&conformance.ErrUnknownEvent{Name: "X"}, `unknown event "X"`},
		{&conformance.ErrConformance{Mismatches: []conformance.Mismatch{{Scenario: "s", Field: "f", Reference: "a", Subject: "b"}}}, "1 mismatch"},
		{&conformance.ErrConformance{}, "no mismatches"},
	}
	for _, c := range cases {
		if !strings.Contains(c.err.Error(), c.want) {
			t.Errorf("%T.Error() = %q, want substring %q", c.err, c.err.Error(), c.want)
		}
	}
}

func TestMismatch_String(t *testing.T) {
	m := conformance.Mismatch{Scenario: "s", Field: "finalState", Reference: "A", Subject: "B"}
	if got := m.String(); !strings.Contains(got, "finalState") || !strings.Contains(got, "reference=") {
		t.Fatalf("Mismatch.String() = %q", got)
	}
}

func TestGenerate_WithMaxDepth(t *testing.T) {
	m := buildDocMachine()
	scs, err := conformance.GenerateScenarios(m, func(e docEvent) string { return e.String() }, conformance.WithMaxDepth(1))
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	for _, sc := range scs {
		if len(sc.Events) > 1 {
			t.Errorf("scenario %q has %d events, want <= 1 under maxDepth=1", sc.Name, len(sc.Events))
		}
	}
}

func TestCompareMachines_IgnoreOptions(t *testing.T) {
	ref := buildDocMachine()
	// Subject diverges on the publish effect only.
	sub := state.Forge[docState, docEvent, *document]("document").
		Action("emit", emit).
		State(draft).State(submitted).State(approved).State(published).State(archived).
		Initial(draft).
		CurrentStateFn(func(d *document) docState { return d.status }).
		Transition(approved).On(publish).GoTo(published).
		Quench()
	scs := []conformance.Scenario{{
		MachineID: "document", Name: "p", InitialState: "Approved",
		Events: []conformance.Event{{Event: "Publish"}},
	}}
	newEntity := func() *document { return &document{status: approved} }
	// Ignoring effects and trace, the two agree on final state.
	if err := conformance.CompareMachines(ref, sub, scs, docCodec(), approved, newEntity,
		conformance.IgnoreEffects(), conformance.IgnoreTrace()); err != nil {
		t.Fatalf("with effects+trace ignored the machines should agree: %v", err)
	}
}

func TestRunAgainst_RecordsEffectsAndTrace(t *testing.T) {
	m := buildDocMachine()
	sc := conformance.Scenario{
		MachineID: "document", InitialState: "Draft",
		Events: []conformance.Event{{Event: "Submit"}, {Event: "Approve"}},
		Assertions: []conformance.Assertion{
			{Type: conformance.AssertFinalState, Expected: "Approved"},
			{Type: conformance.AssertEffectsEmitted, Expected: []string{"emit", "emit"}},
			{Type: conformance.AssertTraceLength, Expected: 2},
			{Type: conformance.AssertNoErrors, Expected: true},
		},
	}
	res := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft)
	if !res.Passed() {
		t.Fatalf("expected pass, got %+v", res.Assertions)
	}
	if len(res.Trace.Steps) != 2 {
		t.Fatalf("trace steps = %d, want 2", len(res.Trace.Steps))
	}
	if res.Trace.Steps[0].Outcome != "Success" {
		t.Fatalf("step 0 outcome = %q, want Success", res.Trace.Steps[0].Outcome)
	}
}

func TestRunAgainst_FailingAssertionsMarkedNotPassed(t *testing.T) {
	m := buildDocMachine()
	sc := conformance.Scenario{
		MachineID: "document", InitialState: "Draft",
		Events: []conformance.Event{{Event: "Submit"}},
		Assertions: []conformance.Assertion{
			{Type: conformance.AssertFinalState, Expected: "Published"}, // wrong
			{Type: conformance.AssertTraceLength, Expected: 99},         // wrong
		},
	}
	res := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft)
	if res.Passed() {
		t.Fatal("expected failing assertions to mark the result not passed")
	}
}
