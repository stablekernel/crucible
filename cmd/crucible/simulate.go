package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
)

// guardFlags collects repeated -guard name=bool tokens into a verdict map. Each
// token seeds the boolean a named guard returns during the simulation; an
// unseeded guard defaults to false.
type guardFlags map[string]bool

func (g guardFlags) String() string { return fmt.Sprint(map[string]bool(g)) }

func (g guardFlags) Set(s string) error {
	idx := strings.Index(s, "=")
	if idx < 1 {
		return fmt.Errorf("malformed -guard %q: want name=true|false", s)
	}
	name := s[:idx]
	val, err := strconv.ParseBool(s[idx+1:])
	if err != nil {
		return fmt.Errorf("malformed -guard %q: %w", s, err)
	}
	g[name] = val
	return nil
}

// simulateStepDTO is the JSON form of one fired event's outcome.
type simulateStepDTO struct {
	Event   string   `json:"event"`
	From    string   `json:"from"`
	To      string   `json:"to"`
	Outcome string   `json:"outcome"`
	Effects []string `json:"effects"`
}

// simulateResultDTO is the JSON form of a whole simulation run.
type simulateResultDTO struct {
	InitialState string            `json:"initialState"`
	Steps        []simulateStepDTO `json:"steps"`
	FinalState   string            `json:"finalState"`
	Effects      []string          `json:"effects"`
}

// collectKnownEvents enumerates every event name referenced by a transition in
// the IR, so the simulation codec can reject an event the machine never declares.
func collectKnownEvents(ir *state.IR[string, string, any]) map[string]bool {
	known := make(map[string]bool)
	for i := range ir.States {
		collectEventsFromState(&ir.States[i], known)
	}
	return known
}

func collectEventsFromState(s *state.State[string, string, any], known map[string]bool) {
	for _, t := range s.Transitions {
		if t.On != "" {
			known[t.On] = true
		}
	}
	for i := range s.Children {
		collectEventsFromState(&s.Children[i], known)
	}
	for i := range s.Regions {
		for j := range s.Regions[i].States {
			collectEventsFromState(&s.Regions[i].States[j], known)
		}
	}
}

// parseEventsFile reads an events file in either form: a bare JSON array of event
// names, or a conformance scenario object whose Events carry the names.
func parseEventsFile(data []byte) ([]string, error) {
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		return arr, nil
	}
	var sc conformance.Scenario
	if err := json.Unmarshal(data, &sc); err != nil {
		return nil, fmt.Errorf("events file: expected JSON array or scenario object: %w", err)
	}
	out := make([]string, len(sc.Events))
	for i, ev := range sc.Events {
		out[i] = ev.Event
	}
	return out, nil
}

// runSimulate fires a sequence of events against a machine assembled from the IR
// and prints the resulting step trace. Guards return seeded verdicts (-guard
// name=bool, default false); actions, reducers, and services are no-ops. The run
// starts from -initial or the IR's declared initial state. A guard-blocked or
// invalid transition is a normal observable outcome (exit 0); an unknown event or
// an action failure is an error (exit 1).
func runSimulate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("simulate", flag.ContinueOnError)
	fs.SetOutput(stderr)

	eventsFlag := fs.String("events", "", "comma-separated event list")
	eventsFile := fs.String("events-file", "", "path to a JSON events file")
	initial := fs.String("initial", "", "override the start state")
	format := fs.String("format", "text", "output format: text or json")
	verdicts := make(guardFlags)
	fs.Var(verdicts, "guard", "seed a guard verdict: name=true|false (repeatable)")

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		emitln(stderr, "usage: crucible simulate <ir.json> -events e1,e2 [-initial S] [-guard name=bool] [-format text|json]")
		return exitUsage
	}

	hasEvents := *eventsFlag != ""
	hasEventsFile := *eventsFile != ""
	if !hasEvents && !hasEventsFile {
		emitln(stderr, "crucible simulate: one of -events or -events-file is required")
		return exitUsage
	}
	if hasEvents && hasEventsFile {
		emitln(stderr, "crucible simulate: -events and -events-file are mutually exclusive")
		return exitUsage
	}

	switch *format {
	case "text", "json":
	default:
		emitf(stderr, "crucible simulate: unknown -format %q (want text or json)\n", *format)
		return exitUsage
	}

	irPath := fs.Arg(0)
	irData, err := loadIR(irPath, os.Stdin)
	if err != nil {
		emitf(stderr, "crucible simulate: %v\n", err)
		return exitError
	}

	var events []string
	if hasEvents {
		events = strings.Split(*eventsFlag, ",")
	} else {
		b, readErr := readInput(*eventsFile, os.Stdin)
		if readErr != nil {
			emitf(stderr, "crucible simulate: read events file: %v\n", readErr)
			return exitError
		}
		events, err = parseEventsFile(b)
		if err != nil {
			emitf(stderr, "crucible simulate: %v\n", err)
			return exitError
		}
	}

	if len(events) == 0 {
		emitln(stderr, "crucible simulate: event list must not be empty")
		return exitUsage
	}
	for _, ev := range events {
		if ev == "" {
			emitln(stderr, "crucible simulate: event names must not be empty")
			return exitUsage
		}
	}

	var startState string
	switch {
	case *initial != "":
		startState = *initial
	case irData.HasInitial:
		startState = fmt.Sprint(irData.Initial)
	default:
		emitln(stderr, "crucible simulate: IR has no initial state; use -initial to specify one")
		return exitError
	}

	sc := conformance.Scenario{
		InitialState: startState,
		Events:       make([]conformance.Event, len(events)),
	}
	for i, ev := range events {
		sc.Events[i] = conformance.Event{Event: ev}
	}

	known := collectKnownEvents(irData)
	codec := conformance.EventCodec[string]{
		Named: func(e string) string { return e },
		Resolve: func(n string) (string, bool) {
			return n, known[n]
		},
	}

	m, err := quenchWith(irData, simulateRegistry(irData, verdicts))
	if err != nil {
		emitf(stderr, "crucible simulate: %v\n", err)
		return exitError
	}

	result := conformance.RunAgainst(m, sc, any(nil), codec, startState)

	exitCode := classifySimulateErr(result.Err)

	if *format == "json" {
		if code := emitSimulateJSON(result, startState, stdout, stderr); code != exitOK {
			return code
		}
	} else {
		emitSimulateText(result, startState, stdout)
	}

	return exitCode
}

// classifySimulateErr maps a run error to an exit code. A guard-blocked or
// invalid transition is a normal observable outcome (exit 0): the simulation ran,
// it just did not advance. An unknown event, an action failure, or any other
// error is a genuine failure (exit 1).
func classifySimulateErr(err error) int {
	if err == nil {
		return exitOK
	}
	var guardFailed *state.GuardFailedError
	var invalidTransition *state.InvalidTransitionError
	var unknownEvent *conformance.ErrUnknownEvent
	var actionFailed *state.ActionFailedError

	switch {
	case errors.As(err, &guardFailed):
		return exitOK
	case errors.As(err, &invalidTransition):
		return exitOK
	case errors.As(err, &unknownEvent):
		return exitError
	case errors.As(err, &actionFailed):
		return exitError
	default:
		return exitError
	}
}

// emitSimulateText prints a human-readable step trace: the start state, one line
// per fired event (event, from -> to, outcome, any error, any effects), and the
// final state.
func emitSimulateText(result conformance.ScenarioResult[string], startState string, stdout io.Writer) {
	emitf(stdout, "initial: %s\n", startState)
	for _, step := range result.Trace.Steps {
		effects := ""
		if len(step.EffectsEmitted) > 0 {
			effects = "   effects: " + strings.Join(step.EffectsEmitted, ", ")
		}
		errSuffix := ""
		if step.Err != "" {
			errSuffix = " (" + step.Err + ")"
		}
		emitf(stdout, "%-10s  %s -> %-10s  %s%s%s\n",
			step.Event,
			step.FromState,
			step.ToState,
			step.Outcome,
			errSuffix,
			effects,
		)
	}
	emitf(stdout, "final: %s\n", result.FinalState)
}

// emitSimulateJSON marshals the run to the simulateResultDTO shape, normalizing
// nil effect slices to empty arrays so the JSON is stable.
func emitSimulateJSON(result conformance.ScenarioResult[string], startState string, stdout, stderr io.Writer) int {
	dto := simulateResultDTO{
		InitialState: startState,
		Steps:        make([]simulateStepDTO, len(result.Trace.Steps)),
		FinalState:   result.FinalState,
		Effects:      result.Effects,
	}
	if dto.Effects == nil {
		dto.Effects = make([]string, 0)
	}
	for i, step := range result.Trace.Steps {
		effects := step.EffectsEmitted
		if effects == nil {
			effects = make([]string, 0)
		}
		dto.Steps[i] = simulateStepDTO{
			Event:   step.Event,
			From:    step.FromState,
			To:      step.ToState,
			Outcome: step.Outcome,
			Effects: effects,
		}
	}
	b, err := json.Marshal(dto)
	if err != nil {
		emitf(stderr, "crucible simulate: marshal JSON: %v\n", err)
		return exitError
	}
	emitf(stdout, "%s\n", b)
	return exitOK
}
