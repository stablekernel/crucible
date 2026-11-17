package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// newerFormat is a journal format above this version's, as a later version would write.
const newerFormat = 9

// newerWriter stamps every Record it keeps with older's format, and the checkpoint too
// when checkpoint is set, as the version that wrote that format would.
type newerWriter struct {
	older      *olderWriter
	checkpoint bool
	*durable.MemStore
}

func (s *newerWriter) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	return s.older.Append(ctx, id, rec, opts...)
}

func (s *newerWriter) Checkpoint(ctx context.Context, id durable.InstanceID, snapshot []byte, throughStep int, opts ...durable.CheckpointOption) error {
	if s.checkpoint {
		return s.older.Checkpoint(ctx, id, snapshot, throughStep, opts...)
	}
	return s.MemStore.Checkpoint(ctx, id, snapshot, throughStep, opts...)
}

func newWriter(format int, checkpoint bool) *newerWriter {
	mem := durable.NewMemStore(durable.WithHistory())
	return &newerWriter{older: &olderWriter{MemStore: mem, format: format}, checkpoint: checkpoint, MemStore: mem}
}

// journalCase is a journal some version wrote: a Start baseline, and, when step is set,
// one service settlement after it.
type journalCase struct {
	name       string
	checkpoint bool // the baseline records format, not this version's
	step       bool // the journal holds a Record, of format
}

// newerCases are the journals a newer version could leave: a newer Record after this
// version's baseline, a newer baseline with an empty tail, and both.
var newerCases = []journalCase{
	{"newer Record", false, true},
	{"newer checkpoint, empty tail", true, false},
	{"newer checkpoint and Record", true, true},
}

// writeJournal starts scn-1 on a store that writes format as c says, and settles its
// service once when c.step is set.
func writeJournal(t *testing.T, c journalCase, format int, a *scnAssigns) (*state.Machine[string, string, svcCtx], *newerWriter, durable.Option[string, string, svcCtx]) {
	t.Helper()
	fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
	m := scnMachine(fn, a)
	store := newWriter(format, c.checkpoint)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	h := scnStart(t, m, store, reg)
	if c.step {
		mustRunService(t, h, state.InvokeID(scnInvoke, "loading", 0))
	}
	return m, store, reg
}

// mustBeNewer fails unless err is the refusal of a journal of newerFormat, which names
// the instance and both formats and wraps none of the package's sentinels.
func mustBeNewer(t *testing.T, what string, err error) {
	t.Helper()
	want := fmt.Sprintf("durable: %q was written by a newer version (journal format %d; this version reads up to 4)", "scn-1", newerFormat)
	if err == nil || err.Error() != want {
		t.Fatalf("%s = %v, want %q", what, err, want)
	}
	for _, sentinel := range []error{
		durable.ErrInstanceNotFound, durable.ErrStepOutOfOrder, durable.ErrStepConflict,
		durable.ErrFenced, durable.ErrCheckpointNotAdvancing, durable.ErrInstanceExists,
		durable.ErrEffectDispatch, durable.ErrStepOutOfRange, durable.ErrEventDataEncoding,
		durable.ErrRecordTooLarge, durable.ErrHandleFailed, durable.ErrReentrantStep,
		durable.ErrNoDispatchStore,
	} {
		if errors.Is(err, sentinel) {
			t.Fatalf("%s's refusal matches %v, want no sentinel", what, sentinel)
		}
	}
}

// A version refuses a journal a newer version wrote, a checkpoint or a Record of a
// format above its own, rather than replay it as its own format. Recover refuses it
// before it restores or replays anything, so no Assign runs again.
func TestJournalFormat_NewerJournal_RecoverRefuses(t *testing.T) {
	for _, c := range newerCases {
		t.Run(c.name, func(t *testing.T) {
			a := &scnAssigns{}
			m, store, reg := writeJournal(t, c, newerFormat, a)
			live := a.done
			_, err := durable.Recover(context.Background(), m, store, "scn-1", reg)
			mustBeNewer(t, "Recover", err)
			if a.done != live {
				t.Fatalf("onDone's Assign ran %d times, want only live's %d", a.done, live)
			}
		})
	}
}

// The stateless Runner.Fire loads the instance as Recover does, so it refuses a newer
// journal too, and appends nothing to it.
func TestJournalFormat_NewerJournal_StatelessFireRefuses(t *testing.T) {
	for _, c := range newerCases {
		t.Run(c.name, func(t *testing.T) {
			m, store, reg := writeJournal(t, c, newerFormat, &scnAssigns{})
			ctx := context.Background()
			_, before, err := store.Load(ctx, "scn-1")
			if err != nil {
				t.Fatal(err)
			}
			_, err = durable.NewRunner(m, store, reg).Fire(ctx, "scn-1", "done")
			mustBeNewer(t, "Runner.Fire", err)
			_, after, err := store.Load(ctx, "scn-1")
			if err != nil || len(after) != len(before) {
				t.Fatalf("the journal holds %d Records (err %v) after the refused Fire, want %d", len(after), err, len(before))
			}
		})
	}
}

// StateAt refuses a newer journal whatever the step, even the baseline, which a newer
// Record after it doesn't reach.
func TestJournalFormat_NewerJournal_StateAtRefuses(t *testing.T) {
	for _, c := range newerCases {
		t.Run(c.name, func(t *testing.T) {
			m, store, reg := writeJournal(t, c, newerFormat, &scnAssigns{})
			steps := []int{durable.BaselineStep}
			if c.step {
				steps = append(steps, 0)
			}
			for _, step := range steps {
				_, err := durable.StateAt(context.Background(), m, store, "scn-1", step, reg)
				mustBeNewer(t, fmt.Sprintf("StateAt(%d)", step), err)
			}
		})
	}
}

// Steps refuses a newer journal, as StateAt does.
func TestJournalFormat_NewerJournal_StepsRefuses(t *testing.T) {
	for _, c := range newerCases {
		t.Run(c.name, func(t *testing.T) {
			_, store, _ := writeJournal(t, c, newerFormat, &scnAssigns{})
			_, err := durable.Steps(context.Background(), store, "scn-1")
			mustBeNewer(t, "Steps", err)
		})
	}
}

// The ceiling refuses only a newer format: a journal of format 0 (the 0.1.0 release), 1
// or 2, this version's, still loads on every read path, with a checkpoint and an empty
// tail or a Record after it.
func TestJournalFormat_OlderAndCurrentJournal_Loads(t *testing.T) {
	for _, format := range []int{0, 1, 2} {
		for _, c := range []journalCase{
			{"checkpoint, empty tail", true, false},
			{"checkpoint and Record", true, true},
		} {
			t.Run(fmt.Sprintf("format %d, %s", format, c.name), func(t *testing.T) {
				ctx := context.Background()
				m, store, reg := writeJournal(t, c, format, &scnAssigns{})
				if _, err := durable.Recover(ctx, m, store, "scn-1", reg); err != nil {
					t.Fatalf("Recover: %v", err)
				}
				if _, err := durable.StateAt(ctx, m, store, "scn-1", durable.BaselineStep, reg); err != nil {
					t.Fatalf("StateAt: %v", err)
				}
				if _, err := durable.Steps(ctx, store, "scn-1"); err != nil {
					t.Fatalf("Steps: %v", err)
				}
				if !c.step { // the instance is still loading, so its onDone event fires
					if _, err := durable.NewRunner(m, store, reg).Fire(ctx, "scn-1", "done"); err != nil {
						t.Fatalf("Runner.Fire: %v", err)
					}
				}
			})
		}
	}
}

// reshapedWriter writes every checkpoint as a later version might: format newerFormat,
// with nextStep changed from a number to a string, so this version's envelope type
// can't unmarshal it.
type reshapedWriter struct {
	*durable.MemStore
}

func (s *reshapedWriter) Checkpoint(ctx context.Context, id durable.InstanceID, snapshot []byte, throughStep int, opts ...durable.CheckpointOption) error {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(snapshot, &env); err != nil {
		return err
	}
	env["format"] = json.RawMessage(strconv.Itoa(newerFormat))
	env["nextStep"] = json.RawMessage(`"x"`)
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.MemStore.Checkpoint(ctx, id, b, throughStep, opts...)
}

// TestJournalFormat_NewerEnvelopeOfAnotherShape_IsRefused: a later version may change
// the type of an envelope field, so this version's envelope type can't unmarshal its
// checkpoint. Every reader must still read the checkpoint's format and refuse it as a
// newer version's, rather than take the bytes for a bare pre-envelope snapshot of
// format 0 and fail later with an obscure error.
func TestJournalFormat_NewerEnvelopeOfAnotherShape_IsRefused(t *testing.T) {
	ctx := context.Background()
	fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
	m := scnMachine(fn, &scnAssigns{})
	store := &reshapedWriter{MemStore: durable.NewMemStore(durable.WithHistory())}
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	scnStart(t, m, store, reg)

	_, err := durable.Recover(ctx, m, store, "scn-1", reg)
	mustBeNewer(t, "Recover", err)
	_, err = durable.NewRunner(m, store, reg).Fire(ctx, "scn-1", "done")
	mustBeNewer(t, "Runner.Fire", err)
	_, err = durable.StateAt(ctx, m, store, "scn-1", durable.BaselineStep, reg)
	mustBeNewer(t, "StateAt", err)
	_, err = durable.Steps(ctx, store, "scn-1")
	mustBeNewer(t, "Steps", err)
}
