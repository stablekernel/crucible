package conformance

import (
	"fmt"
	"strings"
)

// traceEffectNames copies the kernel's recorded effect labels. The kernel
// records each effect as its ref name (or "name:goType"); the conformance layer
// keeps the labels verbatim so a trace stays faithful to what fired.
func traceEffectNames(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	out := make([]string, len(labels))
	copy(out, labels)
	return out
}

// evaluate scores each assertion against the run result, returning one verdict
// per assertion in declaration order.
func evaluate[S comparable](assertions []Assertion, res ScenarioResult[S]) []AssertionResult {
	out := make([]AssertionResult, 0, len(assertions))
	for _, a := range assertions {
		out = append(out, scoreAssertion(a, res))
	}
	return out
}

// scoreAssertion evaluates one assertion. EffectsEmitted compares the emitted
// effect ref names ORDER-SENSITIVELY (a reordered sequence fails); the remaining
// assertions compare scalars.
func scoreAssertion[S comparable](a Assertion, res ScenarioResult[S]) AssertionResult {
	r := AssertionResult{Type: a.Type, Expected: a.Expected}
	switch a.Type {
	case AssertFinalState:
		actual := fmt.Sprint(res.FinalState)
		r.Actual = actual
		r.Pass = fmt.Sprint(a.Expected) == actual
	case AssertEffectsEmitted:
		actual := effectRefNames(res.Effects)
		r.Actual = actual
		r.Pass = sameSequence(toStringSlice(a.Expected), actual)
	case AssertEffectsPayloads:
		actual := res.EffectDetails
		r.Actual = actual
		r.Pass = sameSequence(toStringSlice(a.Expected), actual)
	case AssertTraceLength:
		actual := len(res.Trace.Steps)
		r.Actual = actual
		r.Pass = asInt(a.Expected) == actual
	case AssertNoErrors:
		actual := res.Err == nil
		r.Actual = actual
		want, ok := a.Expected.(bool)
		r.Pass = ok && want == actual
	default:
		r.Actual = nil
		r.Pass = false
	}
	return r
}

// effectRefNames reduces "name:goType" effect labels to their ref name so an
// EffectsEmitted assertion compares against the named refs regardless of the
// concrete Go type the action returned.
func effectRefNames(labels []string) []string {
	out := make([]string, 0, len(labels))
	for _, l := range labels {
		if i := strings.IndexByte(l, ':'); i >= 0 {
			out = append(out, l[:i])
			continue
		}
		out = append(out, l)
	}
	return out
}

// sameSequence reports whether two slices are element-wise equal in order. It is
// the order-sensitive, payload-aware comparison the conformance contract
// requires: a reordered effect sequence or a changed payload rendering is a
// mismatch, not an equivalence. This is deliberately NOT a set comparison — the
// whole point of conformance is to catch an emission-order regression.
func sameSequence(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// toStringSlice coerces an assertion's Expected value (which may arrive as
// []any from JSON or []string from code) to a string slice.
func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, len(t))
		for i, e := range t {
			out[i] = fmt.Sprint(e)
		}
		return out
	default:
		return nil
	}
}

// asInt coerces an assertion's Expected value to an int, tolerating the float64
// JSON numbers decode to.
func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return -1
	}
}
