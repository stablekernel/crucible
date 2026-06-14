package state_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestPaletteCategoryAndExamplesSurface registers a behavior with a category, a
// per-parameter example set, and behavior-level example usages, then asserts they
// all reach the Palette descriptor and serialize under the expected JSON keys.
func TestPaletteCategoryAndExamplesSurface(t *testing.T) {
	reg := state.NewRegistry[order]()
	reg.Guard("minAmount", func(c state.GuardCtx[order]) bool { return c.Entity.amount >= 1 },
		state.Describe("Passes when the amount is at least min.").
			Category("guards").
			Examples(`guard("minAmount", {"min": 5})`).
			ParamSpec(state.ParamSpec{
				Name:     "min",
				Type:     state.IntParam,
				Required: true,
				Examples: []any{1, 5, 100},
			}))

	d := findDescriptor(t, reg.Palette(), state.KindGuard, "minAmount")
	if d.Category != "guards" {
		t.Errorf("category = %q, want %q", d.Category, "guards")
	}
	if !reflect.DeepEqual(d.Examples, []string{`guard("minAmount", {"min": 5})`}) {
		t.Errorf("descriptor examples = %v", d.Examples)
	}
	if len(d.Params) != 1 || !reflect.DeepEqual(d.Params[0].Examples, []any{1, 5, 100}) {
		t.Errorf("param examples = %+v", d.Params)
	}

	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"category":"guards"`, `"examples":["guard`, `"examples":[1,5,100]`} {
		if !strings.Contains(string(b), key) {
			t.Errorf("descriptor JSON %s missing %s", b, key)
		}
	}
}

// TestPaletteWithoutCompletenessFieldsOmitsKeys is the additive guarantee: a
// descriptor registered without a category or examples must serialize to exactly
// the same bytes as before — no spurious "category" or "examples" keys.
func TestPaletteWithoutCompletenessFieldsOmitsKeys(t *testing.T) {
	reg := state.NewRegistry[order]()
	reg.Action("charge", func(state.ActionCtx[order]) (state.Effect, error) { return nil, nil },
		state.Describe("Charges the order.").
			Param("gateway", state.StringParam).
			Writes("Order"))

	d := findDescriptor(t, reg.Palette(), state.KindAction, "charge")
	if d.Category != "" || d.Examples != nil {
		t.Fatalf("expected empty completeness fields, got category=%q examples=%v", d.Category, d.Examples)
	}

	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"kind":"action","name":"charge","description":"Charges the order.","params":[{"name":"gateway","type":"string","required":true}],"writes":["Order"]}`
	if string(b) != want {
		t.Fatalf("descriptor JSON changed:\n got %s\nwant %s", b, want)
	}

	// A param without examples likewise omits the key.
	pb, err := json.Marshal(d.Params[0])
	if err != nil {
		t.Fatalf("marshal param: %v", err)
	}
	if strings.Contains(string(pb), "examples") {
		t.Fatalf("param JSON unexpectedly carries examples: %s", pb)
	}
}

// TestPaletteCompletenessJSONRoundTrip confirms the new fields survive a marshal
// and unmarshal so a loaded palette preserves them.
func TestPaletteCompletenessJSONRoundTrip(t *testing.T) {
	reg := state.NewRegistry[order]()
	reg.Service("notify", nil,
		state.Describe("Notifies the customer.").
			Category("side-effects").
			Examples("notify email").
			ParamSpec(state.ParamSpec{
				Name:     "channel",
				Type:     state.EnumParam,
				Required: true,
				Enum:     []string{"email", "sms"},
				Examples: []any{"email"},
			}))

	in := findDescriptor(t, reg.Palette(), state.KindService, "notify")
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out state.Descriptor
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in %+v\nout %+v", in, out)
	}
}
