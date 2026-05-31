package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// jrnlMachine builds a flat machine that carries an explicit definition version
// and id, so a snapshot taken from it stamps machine identity and the snapshot
// schema version. Its single event mutates context so a trace records both a
// human-readable label and a structured event payload.
func jrnlMachine() *state.Machine[string, string, *snapCtx] {
	return state.Forge[string, string, *snapCtx](
		"flow",
		state.WithMachineVersion("1.2.0"),
		state.WithMachineID("flow-def"),
	).
		Action("bump", func(c state.ActionCtx[*snapCtx]) (state.Effect, error) {
			c.Entity.Count++
			return nil, nil
		}).
		State("idle").
		State("active").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("active").Do("bump").
		Quench()
}

// TestTrace_EventPayloadRoundTrips asserts a Fire records the event as structured,
// JSON-serializable data on the trace (so a future replay can reconstruct it)
// alongside the human-readable Event label, and that the payload round-trips.
func TestTrace_EventPayloadRoundTrips(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("Fire: %v", res.Err)
	}

	if res.Trace.Event != "go" {
		t.Fatalf("event label: want %q, got %q", "go", res.Trace.Event)
	}
	if len(res.Trace.EventPayload) == 0 {
		t.Fatal("EventPayload not recorded on trace")
	}
	var ev string
	if err := json.Unmarshal(res.Trace.EventPayload, &ev); err != nil {
		t.Fatalf("EventPayload not valid JSON: %v", err)
	}
	if ev != "go" {
		t.Fatalf("decoded event payload: want %q, got %q", "go", ev)
	}
}

// TestTrace_JSONRoundTripDeterministic asserts a Trace carrying a structured event
// payload round-trips through JSON byte-identically, so the deterministic trace
// encoding from the emission-ordering contract is preserved.
func TestTrace_JSONRoundTripDeterministic(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	res := inst.Fire(context.Background(), "go")

	first, err := json.Marshal(res.Trace)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back state.Trace
	if uErr := json.Unmarshal(first, &back); uErr != nil {
		t.Fatalf("unmarshal: %v", uErr)
	}
	second, err := json.Marshal(back)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("trace JSON not deterministic across round-trip\n first=%s\nsecond=%s", first, second)
	}
}

// TestSnapshot_StampsVersionIdentity asserts Snapshot stamps the machine
// definition version and id, plus the snapshot-format schema version, so a
// restored instance knows which machine and version it belongs to.
func TestSnapshot_StampsVersionIdentity(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	snap := inst.Snapshot()

	if snap.MachineVersion != "1.2.0" {
		t.Fatalf("MachineVersion: want %q, got %q", "1.2.0", snap.MachineVersion)
	}
	if snap.MachineID != "flow-def" {
		t.Fatalf("MachineID: want %q, got %q", "flow-def", snap.MachineID)
	}
	if snap.SnapshotVersion != state.CurrentSnapshotVersion {
		t.Fatalf("SnapshotVersion: want %d, got %d", state.CurrentSnapshotVersion, snap.SnapshotVersion)
	}
}

// TestSnapshot_VersionIdentityRoundTrips asserts the stamped identity survives a
// JSON round-trip.
func TestSnapshot_VersionIdentityRoundTrips(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	inst.Fire(context.Background(), "go")
	snap := inst.Snapshot()

	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back state.Snapshot[string, string, *snapCtx]
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.MachineVersion != "1.2.0" || back.MachineID != "flow-def" {
		t.Fatalf("identity lost: version=%q id=%q", back.MachineVersion, back.MachineID)
	}
	if back.SnapshotVersion != state.CurrentSnapshotVersion {
		t.Fatalf("snapshot version lost: %d", back.SnapshotVersion)
	}
}

// TestRestore_VersionMatrix exercises the lenient restore-version posture: a
// compatible (same-major, <= current minor) SnapshotVersion is accepted; a major
// mismatch is rejected with a typed *SnapshotVersionError. A zero version (a
// pre-versioning snapshot) is treated as the current version and accepted.
func TestRestore_VersionMatrix(t *testing.T) {
	m := jrnlMachine()
	base := func() state.Snapshot[string, string, *snapCtx] {
		inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
		inst.Fire(context.Background(), "go")
		return inst.Snapshot()
	}

	tests := []struct {
		name       string
		version    int
		wantReject bool
	}{
		{"current accepted", state.CurrentSnapshotVersion, false},
		{"zero treated as current accepted", 0, false},
		{"future major rejected", (state.CurrentSnapshotVersion + 1) * 1000, true},
		{"negative rejected", -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snap := base()
			snap.SnapshotVersion = tc.version
			_, err := m.Restore(snap)
			if tc.wantReject {
				if err == nil {
					t.Fatalf("want rejection for version %d, got nil", tc.version)
				}
				var ve *state.SnapshotVersionError
				if !errors.As(err, &ve) {
					t.Fatalf("want *SnapshotVersionError, got %T: %v", err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("want accept for version %d, got %v", tc.version, err)
			}
		})
	}
}

// TestRestore_MachineVersionMismatch asserts the machine definition version is
// recorded and, by default, a mismatch is advisory (accepted) so version stamping
// is non-breaking, while RejectMachineVersionMismatch opts into strict rejection.
func TestRestore_MachineVersionMismatch(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	inst.Fire(context.Background(), "go")
	snap := inst.Snapshot()
	snap.MachineVersion = "9.9.9"

	if _, err := m.Restore(snap); err != nil {
		t.Fatalf("default posture must accept a machine-version mismatch, got %v", err)
	}

	_, err := m.Restore(snap, state.RejectMachineVersionMismatch[string]())
	if err == nil {
		t.Fatal("RejectMachineVersionMismatch must reject a mismatched machine version")
	}
	var ve *state.SnapshotVersionError
	if !errors.As(err, &ve) {
		t.Fatalf("want *SnapshotVersionError, got %T: %v", err, err)
	}
}

// TestSnapshot_JournalRoundTrips asserts the reserved replay journal round-trips
// both empty and populated, carrying a structured nondeterministic result keyed by
// a stable correlation id and the Fire ordinal it resolved at.
func TestSnapshot_JournalRoundTrips(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	inst.Fire(context.Background(), "go")

	// Empty journal round-trips as absent.
	emptySnap := inst.Snapshot()
	if emptySnap.Journal != nil {
		t.Fatalf("fresh snapshot must reserve an empty journal, got %v", emptySnap.Journal)
	}
	b, err := json.Marshal(emptySnap)
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	var backEmpty state.Snapshot[string, string, *snapCtx]
	if uErr := json.Unmarshal(b, &backEmpty); uErr != nil {
		t.Fatalf("unmarshal empty: %v", uErr)
	}
	if len(backEmpty.Journal) != 0 {
		t.Fatalf("empty journal must stay empty, got %v", backEmpty.Journal)
	}

	// Populated journal round-trips losslessly.
	snap := inst.Snapshot()
	snap.Journal = []state.JournalEntry{
		{
			Step:          1,
			Kind:          state.JournalServiceResult,
			CorrelationID: "flow:active#0",
			Payload:       json.RawMessage(`{"ok":true}`),
			ClockUnixNano: 1700000000000000000,
		},
	}
	pb, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal populated: %v", err)
	}
	var back state.Snapshot[string, string, *snapCtx]
	if err := json.Unmarshal(pb, &back); err != nil {
		t.Fatalf("unmarshal populated: %v", err)
	}
	if !reflect.DeepEqual(back.Journal, snap.Journal) {
		t.Fatalf("journal not round-tripped\n want=%+v\n  got=%+v", snap.Journal, back.Journal)
	}
}

// TestSnapshot_InFlightSlotsRoundTrip asserts the reserved in-flight invoked
// service and actor mailbox slots round-trip empty and populated, so a future
// distributed/async resume has a place to carry them.
func TestSnapshot_InFlightSlotsRoundTrip(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	inst.Fire(context.Background(), "go")

	empty := inst.Snapshot()
	if empty.InFlightServices != nil || empty.Mailboxes != nil {
		t.Fatalf("fresh snapshot must reserve empty in-flight slots, got services=%v mailboxes=%v",
			empty.InFlightServices, empty.Mailboxes)
	}

	snap := inst.Snapshot()
	snap.InFlightServices = []state.InFlightService{
		{ID: "flow:active#0", Src: "fetch", Input: json.RawMessage(`{"q":1}`), OnDone: "done", OnError: "fail"},
	}
	snap.Mailboxes = map[string][]json.RawMessage{
		"child-1": {json.RawMessage(`{"msg":"a"}`), json.RawMessage(`{"msg":"b"}`)},
	}
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back state.Snapshot[string, string, *snapCtx]
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(back.InFlightServices, snap.InFlightServices) {
		t.Fatalf("in-flight services not round-tripped\n want=%+v\n  got=%+v",
			snap.InFlightServices, back.InFlightServices)
	}
	if !reflect.DeepEqual(back.Mailboxes, snap.Mailboxes) {
		t.Fatalf("mailboxes not round-tripped\n want=%+v\n  got=%+v", snap.Mailboxes, back.Mailboxes)
	}
}

// badEvent is a comparable event type whose JSON encoding always fails, so a Fire
// driven by it records the human-readable Event label but omits EventPayload —
// proving the payload field is additive and a non-encodable event never breaks the
// trace or the deterministic round-trip.
type badEvent struct{ Name string }

func (badEvent) MarshalJSON() ([]byte, error) { return nil, errors.New("unencodable event") }

func (b badEvent) String() string { return b.Name }

// TestTrace_EventPayloadOmittedForUnencodableEvent asserts an event with no JSON
// form yields no EventPayload while still recording the Event label, and the trace
// still round-trips.
func TestTrace_EventPayloadOmittedForUnencodableEvent(t *testing.T) {
	m := state.Forge[string, badEvent, *snapCtx]("bad").
		State("idle").
		State("active").Final().
		Initial("idle").
		Transition("idle").On(badEvent{Name: "go"}).GoTo("active").
		Quench()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	res := inst.Fire(context.Background(), badEvent{Name: "go"})
	if res.Err != nil {
		t.Fatalf("Fire: %v", res.Err)
	}
	if res.Trace.Event != "go" {
		t.Fatalf("event label: want %q, got %q", "go", res.Trace.Event)
	}
	if res.Trace.EventPayload != nil {
		t.Fatalf("EventPayload must be omitted for an unencodable event, got %s", res.Trace.EventPayload)
	}
	if _, err := json.Marshal(res.Trace); err != nil {
		t.Fatalf("trace with omitted payload must still marshal: %v", err)
	}
}

// TestRestore_EmptyConfigurationWithVersionIdentity asserts the version posture is
// validated even on the empty-configuration restore path (current-state fallback).
func TestRestore_EmptyConfigurationWithVersionIdentity(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	snap := inst.Snapshot()
	snap.Configuration = nil // force the current-state fallback path
	snap.SnapshotVersion = (state.CurrentSnapshotVersion + 1) * 1000
	if _, err := m.Restore(snap); err == nil {
		t.Fatal("major snapshot-format mismatch must reject even on the empty-config path")
	}
}

// TestSnapshot_RestoreCarriesIdentityAndReservedSlots asserts a full marshal →
// unmarshal → restore cycle preserves identity and reserved slots and yields a
// behaviorally identical instance.
func TestSnapshot_RestoreCarriesIdentityAndReservedSlots(t *testing.T) {
	m := jrnlMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	inst.Fire(context.Background(), "go")

	snap := inst.Snapshot()
	b, err := state.MarshalSnapshot(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	back, err := state.UnmarshalSnapshot[string, string, *snapCtx](b)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.MachineVersion != "1.2.0" || back.SnapshotVersion != state.CurrentSnapshotVersion {
		t.Fatalf("identity lost through MarshalSnapshot: %+v", back)
	}
	restored, err := m.Restore(back)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.Current() != "active" {
		t.Fatalf("restored current: want %q, got %q", "active", restored.Current())
	}
}
