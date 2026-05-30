package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file exercises history pseudo-states (shallow and deep).
//
// Shallow history is built through the DSL on a flat compound: a Playing
// superstate whose direct children are Audio and Video. Resuming via a
// shallow-history target restores the last active direct child.
//
// Deep history needs a nested leaf to distinguish it from shallow, which means a
// superstate inside a superstate. The DSL gates nested superstates in v1, but
// the IR (structs) supports arbitrary depth, so the deep-history machine is
// authored as IR and loaded via Provide — the same path a JSON-authored machine
// takes. There, Playing nests an Audio compound (Lo/Hi); deep history restores
// the exact nested leaf, shallow would only restore the Audio child.

type playState int

const (
	stopped playState = iota
	playing
	audio
	lo
	hi
	video
	paused
	histH
)

func (s playState) String() string {
	switch s {
	case stopped:
		return "Stopped"
	case playing:
		return "Playing"
	case audio:
		return "Audio"
	case lo:
		return "Lo"
	case hi:
		return "Hi"
	case video:
		return "Video"
	case paused:
		return "Paused"
	case histH:
		return "H"
	default:
		return "play?"
	}
}

type playEvent int

const (
	evPlay playEvent = iota
	evHiFi
	evLoFi
	evVideo
	evPause
	evResume
)

func (e playEvent) String() string {
	switch e {
	case evPlay:
		return "Play"
	case evHiFi:
		return "HiFi"
	case evLoFi:
		return "LoFi"
	case evVideo:
		return "Video"
	case evPause:
		return "Pause"
	case evResume:
		return "Resume"
	default:
		return "ev?"
	}
}

type player struct{ State playState }

func fire(t *testing.T, inst *state.Instance[playState, playEvent, *player], ev playEvent) {
	t.Helper()
	res := inst.Fire(context.Background(), ev)
	if res.Err != nil {
		t.Fatalf("Fire(%v) from %v: %v", ev, inst.Current(), res.Err)
	}
}

// buildShallowMachine forges (via the DSL) a flat Playing compound with direct
// children Audio and Video and a shallow-history Resume target.
func buildShallowMachine(t *testing.T, dflt *playState) *state.Machine[playState, playEvent, *player] {
	t.Helper()
	b := state.Forge[playState, playEvent, *player]("player-shallow").
		State(stopped).
		Transition(stopped).On(evPlay).GoTo(playing).
		SuperState(playing).
		Initial(audio).
		History(histH, state.HistoryShallow)
	if dflt != nil {
		b = b.DefaultTo(*dflt)
	}
	return b.
		SubState(audio).
		On(evVideo).GoTo(video).
		SubState(video).
		On(evHiFi).GoTo(audio).
		Transition(playing).On(evPause).GoTo(paused).
		EndSuperState().
		Transition(paused).On(evResume).GoTo(histH).
		Transition(stopped).On(evResume).GoTo(histH).
		Initial(stopped).
		CurrentStateFn(func(p *player) playState { return p.State }).
		Quench()
}

// deepIR builds the nested deep-history machine as IR: Playing nests an Audio
// compound (Lo/Hi) plus a deep-history pseudo-state. dft optionally sets the
// history default target.
func deepIR(dft *playState) state.IR[playState, playEvent, *player] {
	initAudio := audio
	initLo := lo
	st := func(name playState, ts ...state.Transition[playState, playEvent, *player]) state.State[playState, playEvent, *player] {
		return state.State[playState, playEvent, *player]{Name: name, Transitions: ts}
	}
	tr := func(from playState, on playEvent, to playState) state.Transition[playState, playEvent, *player] {
		return state.Transition[playState, playEvent, *player]{From: from, On: on, To: to}
	}
	histState := state.State[playState, playEvent, *player]{Name: histH, HistoryType: state.HistoryDeep}
	if dft != nil {
		histState.HistoryDefault = dft
	}
	audioCompound := state.State[playState, playEvent, *player]{
		Name:         audio,
		InitialChild: &initLo,
		Children: []state.State[playState, playEvent, *player]{
			st(lo, tr(lo, evHiFi, hi)),
			st(hi, tr(hi, evLoFi, lo)),
		},
	}
	playingCompound := state.State[playState, playEvent, *player]{
		Name:         playing,
		InitialChild: &initAudio,
		Children: []state.State[playState, playEvent, *player]{
			audioCompound,
			histState,
		},
		Transitions: []state.Transition[playState, playEvent, *player]{tr(playing, evPause, paused)},
	}
	return state.IR[playState, playEvent, *player]{
		Name: "player-deep",
		States: []state.State[playState, playEvent, *player]{
			st(stopped, tr(stopped, evPlay, playing), tr(stopped, evResume, histH)),
			playingCompound,
			st(paused, tr(paused, evResume, histH)),
		},
		Initial:    stopped,
		HasInitial: true,
	}
}

func buildDeepMachine(t *testing.T, dft *playState) *state.Machine[playState, playEvent, *player] {
	t.Helper()
	ir := deepIR(dft)
	return ir.Provide(state.NewRegistry[*player]()).Quench()
}

// TestShallowHistory_RestoresDirectChild: after navigating Playing into Video,
// pausing, and resuming via a shallow-history target, the machine re-enters
// Video (the remembered direct child) rather than Playing's initial Audio.
func TestShallowHistory_RestoresDirectChild(t *testing.T) {
	m := buildShallowMachine(t, nil)
	inst := m.Cast(&player{}, state.WithInitialState(stopped))

	fire(t, inst, evPlay) // -> Playing -> Audio
	if got := inst.Current(); got != audio {
		t.Fatalf("after Play: current=%v want Audio", got)
	}
	fire(t, inst, evVideo) // Audio -> Video (another direct child)
	if got := inst.Current(); got != video {
		t.Fatalf("after Video: current=%v want Video", got)
	}
	fire(t, inst, evPause) // exit Playing -> Paused (records history=Video)
	if got := inst.Current(); got != paused {
		t.Fatalf("after Pause: current=%v want Paused", got)
	}
	fire(t, inst, evResume) // -> H -> restore Video
	if got := inst.Current(); got != video {
		t.Fatalf("shallow history restore: current=%v want Video", got)
	}
}

// TestDeepHistory_RestoresNestedLeaf: after navigating into the nested Audio.Hi
// leaf, pausing, and resuming via a deep-history target, the machine restores
// Audio.Hi exactly — not Audio's initial Lo.
func TestDeepHistory_RestoresNestedLeaf(t *testing.T) {
	m := buildDeepMachine(t, nil)
	inst := m.Cast(&player{}, state.WithInitialState(stopped))

	fire(t, inst, evPlay) // -> Playing -> Audio -> Lo
	if got := inst.Current(); got != lo {
		t.Fatalf("after Play: current=%v want Lo", got)
	}
	fire(t, inst, evHiFi) // Audio: Lo -> Hi (nested leaf)
	if got := inst.Current(); got != hi {
		t.Fatalf("after HiFi: current=%v want Hi", got)
	}
	fire(t, inst, evPause)  // exit Playing (records deep config Audio.Hi)
	fire(t, inst, evResume) // -> H -> deep restore Hi
	if got := inst.Current(); got != hi {
		t.Fatalf("deep history restore: current=%v want Hi", got)
	}
}

// TestDeepHistory_ShallowFlavorDiffers: the same nested machine but the history
// pseudo-state is shallow — resuming after Audio.Hi restores the Audio child and
// descends to its initial Lo, proving shallow != deep on a nested compound.
func TestDeepHistory_ShallowFlavorDiffers(t *testing.T) {
	ir := deepIR(nil)
	// Flip the history pseudo-state to shallow.
	for i := range ir.States {
		if ir.States[i].Name == playing {
			for j := range ir.States[i].Children {
				if ir.States[i].Children[j].Name == histH {
					ir.States[i].Children[j].HistoryType = state.HistoryShallow
				}
			}
		}
	}
	m := ir.Provide(state.NewRegistry[*player]()).Quench()
	inst := m.Cast(&player{}, state.WithInitialState(stopped))

	fire(t, inst, evPlay)
	fire(t, inst, evHiFi)   // Audio.Lo -> Audio.Hi
	fire(t, inst, evPause)  // record Audio (direct child) + deep Hi
	fire(t, inst, evResume) // shallow: restore Audio, descend to Lo
	if got := inst.Current(); got != lo {
		t.Fatalf("shallow flavor: current=%v want Lo (initial of restored Audio)", got)
	}
}

// TestHistory_NoPriorHistoryUsesDefault: a history target whose compound has
// never been exited falls back to the declared default target.
func TestHistory_NoPriorHistoryUsesDefault(t *testing.T) {
	dflt := video
	m := buildShallowMachine(t, &dflt)
	inst := m.Cast(&player{}, state.WithInitialState(stopped))
	// Resume straight from Stopped without ever entering Playing.
	fire(t, inst, evResume) // no recorded history -> default target Video
	if got := inst.Current(); got != video {
		t.Fatalf("no-history default: current=%v want Video", got)
	}
}

// TestHistory_NoPriorHistoryNoDefaultUsesInitial: without a declared default the
// fallback is the compound's InitialChild.
func TestHistory_NoPriorHistoryNoDefaultUsesInitial(t *testing.T) {
	m := buildShallowMachine(t, nil)
	inst := m.Cast(&player{}, state.WithInitialState(stopped))
	// Resume straight from Stopped: never entered Playing, no default declared.
	fire(t, inst, evResume) // fallback to Playing's InitialChild Audio
	if got := inst.Current(); got != audio {
		t.Fatalf("no-history no-default: current=%v want Audio (compound initial)", got)
	}
}

// TestHistory_IRRoundTrip: a machine with a history pseudo-state serializes and
// reloads losslessly, preserving HistoryType and HistoryDefault, and still
// restores history at runtime after the round-trip.
func TestHistory_IRRoundTrip(t *testing.T) {
	dft := audio
	m := buildDeepMachine(t, &dft)

	first, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[playState, playEvent, *player](first)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	second, err := ir.Provide(state.NewRegistry[*player]()).Quench().ToJSON()
	if err != nil {
		t.Fatalf("reserialize: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("history IR not byte-stable under round-trip:\n first=%s\nsecond=%s", first, second)
	}

	// The reloaded machine still restores deep history at runtime.
	rm := ir.Provide(state.NewRegistry[*player]()).Quench()
	inst := rm.Cast(&player{}, state.WithInitialState(stopped))
	fire(t, inst, evPlay)
	fire(t, inst, evHiFi)   // Audio.Lo -> Audio.Hi
	fire(t, inst, evPause)  // record deep Hi
	fire(t, inst, evResume) // deep restore Hi
	if got := inst.Current(); got != hi {
		t.Fatalf("reloaded deep history restore: current=%v want Hi", got)
	}
}

// TestHistory_LintOutsideCompound: declaring a history state outside a compound
// (top level) is a Quench-time programmer error.
func TestHistory_LintOutsideCompound(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected Quench to panic on a top-level history state")
		}
	}()
	state.Forge[playState, playEvent, *player]("bad").
		State(stopped).
		History(histH, state.HistoryShallow). // outside any SuperState
		Initial(stopped).
		CurrentStateFn(func(p *player) playState { return p.State }).
		Quench()
}

// TestHistory_LintInRegion: a history pseudo-state nested in a parallel region
// (not a compound) is a Quench-time programmer error, caught on the IR path.
func TestHistory_LintInRegion(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected Quench to panic on a history state inside a region")
		}
	}()
	initLo := lo
	ir := state.IR[playState, playEvent, *player]{
		Name: "bad-region",
		States: []state.State[playState, playEvent, *player]{
			{
				Name: playing,
				Regions: []state.Region[playState, playEvent, *player]{
					{
						Name:         "R",
						InitialChild: &initLo,
						States: []state.State[playState, playEvent, *player]{
							{Name: lo},
							{Name: histH, HistoryType: state.HistoryShallow},
						},
					},
				},
			},
		},
		Initial:    playing,
		HasInitial: true,
	}
	ir.Provide(state.NewRegistry[*player]()).Quench()
}
