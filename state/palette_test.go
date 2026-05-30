package state_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// order is the entity the palette tests register behavior against.
type order struct {
	amount   int
	currency string
}

// buildPaletteRegistry registers a representative mix of described and
// undescribed guards, actions, services, and actor behaviors.
func buildPaletteRegistry() *state.Registry[order] {
	reg := state.NewRegistry[order]()

	reg.Guard("minAmount", func(c state.GuardCtx[order]) bool {
		return c.Entity.amount >= 1 && c.Entity.currency != ""
	},
		state.Describe("Passes when the amount is at least min.").
			Param("min", state.IntParam).
			OptionalParam("currency", state.StringParam).
			Reads("Order"))
	reg.Guard("always", func(state.GuardCtx[order]) bool { return true }) // no descriptor

	reg.Action("charge", func(state.ActionCtx[order]) (state.Effect, error) { return nil, nil },
		state.Describe("Charges the order.").
			Param("gateway", state.StringParam).
			Writes("Order"))
	reg.Action("log", func(state.ActionCtx[order]) (state.Effect, error) { return nil, nil }) // no descriptor

	reg.Service("settle", nil) // no descriptor
	reg.Service("notify", nil,
		state.Describe("Notifies the customer.").
			EnumParam("channel", "email", "sms"))

	reg.Actor("worker",
		state.Describe("A worker child machine.").
			Param("queue", state.StringParam))
	reg.Actor("auditor") // no descriptor

	return reg
}

// findDescriptor returns the descriptor with the given kind and name.
func findDescriptor(t *testing.T, pal []state.Descriptor, kind state.DescriptorKind, name string) state.Descriptor {
	t.Helper()
	for _, d := range pal {
		if d.Kind == kind && d.Name == name {
			return d
		}
	}
	t.Fatalf("descriptor %s/%s not found in palette", kind, name)
	return state.Descriptor{}
}

func TestPaletteIncludesAllRegisteredKinds(t *testing.T) {
	pal := buildPaletteRegistry().Palette()

	if got, want := len(pal), 8; got != want {
		t.Fatalf("palette length = %d, want %d", got, want)
	}

	want := map[state.DescriptorKind][]string{
		state.KindGuard:   {"always", "minAmount"},
		state.KindAction:  {"charge", "log"},
		state.KindService: {"notify", "settle"},
		state.KindActor:   {"auditor", "worker"},
	}
	got := map[state.DescriptorKind][]string{}
	for _, d := range pal {
		got[d.Kind] = append(got[d.Kind], d.Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("palette kinds = %v, want %v", got, want)
	}
}

func TestPaletteDescriptorContents(t *testing.T) {
	pal := buildPaletteRegistry().Palette()

	g := findDescriptor(t, pal, state.KindGuard, "minAmount")
	if g.Description != "Passes when the amount is at least min." {
		t.Errorf("guard description = %q", g.Description)
	}
	if !reflect.DeepEqual(g.Reads, []string{"Order"}) {
		t.Errorf("guard reads = %v", g.Reads)
	}
	wantParams := []state.ParamSpec{
		{Name: "min", Type: state.IntParam, Required: true},
		{Name: "currency", Type: state.StringParam},
	}
	if !reflect.DeepEqual(g.Params, wantParams) {
		t.Errorf("guard params = %+v, want %+v", g.Params, wantParams)
	}

	// Enum param carries its allowed values.
	svc := findDescriptor(t, pal, state.KindService, "notify")
	if len(svc.Params) != 1 || svc.Params[0].Type != state.EnumParam ||
		!reflect.DeepEqual(svc.Params[0].Enum, []string{"email", "sms"}) {
		t.Errorf("enum param = %+v", svc.Params)
	}

	act := findDescriptor(t, pal, state.KindAction, "charge")
	if !reflect.DeepEqual(act.Writes, []string{"Order"}) {
		t.Errorf("action writes = %v", act.Writes)
	}
}

func TestPaletteMinimalDescriptorForUndescribed(t *testing.T) {
	pal := buildPaletteRegistry().Palette()

	for _, tc := range []struct {
		kind state.DescriptorKind
		name string
	}{
		{state.KindGuard, "always"},
		{state.KindAction, "log"},
		{state.KindService, "settle"},
		{state.KindActor, "auditor"},
	} {
		d := findDescriptor(t, pal, tc.kind, tc.name)
		if d.Description != "" || len(d.Params) != 0 || len(d.Reads) != 0 || len(d.Writes) != 0 {
			t.Errorf("%s/%s should be minimal, got %+v", tc.kind, tc.name, d)
		}
	}
}

func TestPaletteDeterministicOrder(t *testing.T) {
	first := buildPaletteRegistry().Palette()
	for i := 0; i < 20; i++ {
		got := buildPaletteRegistry().Palette()
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("palette order not deterministic at run %d:\n%v\n!=\n%v", i, got, first)
		}
	}
	// Verify sort: kind ascending, then name ascending.
	for i := 1; i < len(first); i++ {
		prev, cur := first[i-1], first[i]
		if prev.Kind > cur.Kind || (prev.Kind == cur.Kind && prev.Name > cur.Name) {
			t.Fatalf("palette not sorted at %d: %v then %v", i, prev, cur)
		}
	}
}

func TestDescribeParamSpecHelper(t *testing.T) {
	reg := state.NewRegistry[order]()
	reg.Action("retry", func(state.ActionCtx[order]) (state.Effect, error) { return nil, nil },
		state.Describe("Retries with a configurable backoff.").
			ParamSpec(state.ParamSpec{
				Name:        "backoff",
				Type:        state.DurationParam,
				Description: "delay before retry",
				Default:     "500ms",
			}).
			Param("attempts", state.IntParam))

	d := findDescriptor(t, reg.Palette(), state.KindAction, "retry")
	want := []state.ParamSpec{
		{Name: "backoff", Type: state.DurationParam, Description: "delay before retry", Default: "500ms"},
		{Name: "attempts", Type: state.IntParam, Required: true},
	}
	if !reflect.DeepEqual(d.Params, want) {
		t.Fatalf("params = %+v, want %+v", d.Params, want)
	}
}

func TestPaletteEmptyRegistry(t *testing.T) {
	if pal := state.NewRegistry[order]().Palette(); len(pal) != 0 {
		t.Fatalf("empty registry palette = %v, want empty", pal)
	}
}

func TestRegisterWithoutDescriptorStillBinds(t *testing.T) {
	// Backward-compat: the no-option registration path still registers a working
	// implementation that Quench binds and Fire runs.
	m := state.Forge[string, string, order]("checkout").
		Guard("ok", func(state.GuardCtx[order]) bool { return true }).
		Action("noop", func(state.ActionCtx[order]) (state.Effect, error) { return "done", nil }).
		State("start").Initial("start").
		Transition("start").On("go").GoTo("end").When("ok").Do("noop").
		State("end").
		Quench()

	inst := m.Cast(order{}, state.WithInitialState("start"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("fire errored: %v", res.Err)
	}
	if res.NewState != "end" {
		t.Fatalf("new state = %q, want end", res.NewState)
	}
	pal := m.Palette()
	if len(pal) != 2 {
		t.Fatalf("machine palette length = %d, want 2", len(pal))
	}
}

func TestParamSpecJSONRoundTrip(t *testing.T) {
	in := state.ParamSpec{
		Name:        "channel",
		Type:        state.EnumParam,
		Required:    true,
		Description: "delivery channel",
		Default:     "email",
		Enum:        []string{"email", "sms"},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out state.ParamSpec
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\nin  %+v\nout %+v", in, out)
	}
}

func TestDescriptorJSONRoundTrip(t *testing.T) {
	pal := buildPaletteRegistry().Palette()
	b, err := json.Marshal(pal)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out []state.Descriptor
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(pal, out) {
		t.Fatalf("descriptor round trip mismatch:\n%+v\n!=\n%+v", pal, out)
	}
}

func TestBuilderPaletteSurfacesDescriptors(t *testing.T) {
	b := state.Forge[string, string, order]("m").
		Guard("minAmount", func(c state.GuardCtx[order]) bool { return c.Entity.amount >= 1 },
			state.Describe("Passes when the amount is at least min.").
				Param("min", state.IntParam).
				OptionalParam("currency", state.StringParam).
				Reads("Order"))
	bp := b.Palette()
	if len(bp) != 1 || bp[0].Name != "minAmount" || bp[0].Kind != state.KindGuard {
		t.Fatalf("builder palette = %+v", bp)
	}
}

func TestProvideCarriesPalette(t *testing.T) {
	reg := state.NewRegistry[order]()
	reg.Guard("ok", func(state.GuardCtx[order]) bool { return true },
		state.Describe("always ok").Param("x", state.IntParam))
	reg.Actor("child", state.Describe("a child").Param("q", state.StringParam))

	ir := &state.IR[string, string, order]{
		Name:       "loaded",
		Initial:    "a",
		HasInitial: true,
		States: []state.State[string, string, order]{
			{Name: "a", Transitions: []state.Transition[string, string, order]{
				{From: "a", To: "b", On: "go", Guards: []state.Ref{{Name: "ok"}}},
			}},
			{Name: "b"},
		},
	}
	m := ir.Provide(reg).Quench()

	pal := m.Palette()
	if len(pal) != 2 {
		t.Fatalf("provided machine palette = %+v, want 2 entries", pal)
	}
	findDescriptor(t, pal, state.KindGuard, "ok")
	findDescriptor(t, pal, state.KindActor, "child")
}

func TestBuiltinPaletteExcludedFromPalette(t *testing.T) {
	// The built-ins never appear in a consumer registry's Palette.
	pal := buildPaletteRegistry().Palette()
	for _, d := range pal {
		if d.Name == "crucible.spawn" || d.Name == "stateIn" {
			t.Fatalf("built-in %q leaked into Palette", d.Name)
		}
	}

	bp := state.BuiltinPalette()
	if len(bp) == 0 {
		t.Fatal("BuiltinPalette is empty")
	}
	// BuiltinPalette includes the stateIn guard and the spawn action.
	var hasStateIn, hasSpawn bool
	for _, d := range bp {
		if d.Kind == state.KindGuard && d.Name == "stateIn" {
			hasStateIn = true
		}
		if d.Kind == state.KindAction && d.Name == "crucible.spawn" {
			hasSpawn = true
		}
	}
	if !hasStateIn || !hasSpawn {
		t.Fatalf("BuiltinPalette missing expected built-ins: %+v", bp)
	}
	// Deterministic + JSON round-trips.
	b, err := json.Marshal(bp)
	if err != nil {
		t.Fatalf("marshal builtin palette: %v", err)
	}
	var out []state.Descriptor
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal builtin palette: %v", err)
	}
	if !reflect.DeepEqual(bp, out) {
		t.Fatal("builtin palette round trip mismatch")
	}
}
