package symbolic

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// fieldKind is how a context field is analyzed: as a number line (intervals) or a
// discrete value set, or not at all (unknown — the analyzer cannot constrain it).
type fieldKind int

const (
	kindUnknown fieldKind = iota
	kindNumeric
	kindDiscrete
)

// schemaKinds maps each schema field path to how it is analyzed. Int/float fields
// are numeric; string/bool/enum are discrete; duration/time and anything absent are
// unknown (left unconstrained — the analyzer never invents a contradiction it cannot
// prove).
func schemaKinds(schema state.ContextSchema) map[string]fieldKind {
	kinds := make(map[string]fieldKind, len(schema.Fields))
	for _, f := range schema.Fields {
		switch f.Kind {
		case state.SchemaInt, state.SchemaFloat:
			kinds[f.Name] = kindNumeric
		case state.SchemaString, state.SchemaBool, state.SchemaEnum:
			kinds[f.Name] = kindDiscrete
		default:
			kinds[f.Name] = kindUnknown
		}
	}
	return kinds
}

// interval is the satisfiable range of a numeric field within one clause: a lower
// and upper bound (each present or open, inclusive or exclusive) plus the values a
// not-equal excludes.
type interval struct {
	lo, hi       float64
	loInc, hiInc bool
	loSet, hiSet bool
	neqs         map[float64]struct{}
}

func newInterval() *interval { return &interval{neqs: map[float64]struct{}{}} }

// tightenLow raises the lower bound to v (keeping the stricter of the existing and
// new bound; when the values tie, the bound is inclusive only if both are).
func (iv *interval) tightenLow(v float64, inclusive bool) {
	if !iv.loSet || v > iv.lo || (v == iv.lo && !inclusive) {
		iv.lo, iv.loInc, iv.loSet = v, inclusive, true
	}
}

// tightenHigh lowers the upper bound to v.
func (iv *interval) tightenHigh(v float64, inclusive bool) {
	if !iv.hiSet || v < iv.hi || (v == iv.hi && !inclusive) {
		iv.hi, iv.hiInc, iv.hiSet = v, inclusive, true
	}
}

// empty reports whether the interval admits no value: bounds crossed, an empty open
// span, or a single admissible point that a not-equal excludes.
func (iv *interval) empty() bool {
	if iv.loSet && iv.hiSet {
		if iv.lo > iv.hi {
			return true
		}
		if iv.lo == iv.hi {
			if !iv.loInc || !iv.hiInc {
				return true // a single point reachable only exclusively
			}
			if _, ok := iv.neqs[iv.lo]; ok {
				return true // the only admissible point is excluded
			}
		}
	}
	return false
}

// discrete is the satisfiable value set of a discrete field within one clause:
// the values an equality requires, the values a not-equal excludes, and the
// membership set an `in` restricts to (nil means unconstrained).
type discrete struct {
	required   map[string]struct{} // == values (more than one distinct ⇒ contradiction)
	excluded   map[string]struct{} // != values
	membership map[string]struct{} // intersection of `in` sets; nil = unconstrained
}

func newDiscrete() *discrete {
	return &discrete{required: map[string]struct{}{}, excluded: map[string]struct{}{}}
}

func (d *discrete) require(v string) { d.required[v] = struct{}{} }
func (d *discrete) exclude(v string) { d.excluded[v] = struct{}{} }

// restrict intersects the membership set with vals (the first `in` establishes it).
func (d *discrete) restrict(vals map[string]struct{}) {
	if d.membership == nil {
		d.membership = vals
		return
	}
	next := map[string]struct{}{}
	for v := range d.membership {
		if _, ok := vals[v]; ok {
			next[v] = struct{}{}
		}
	}
	d.membership = next
}

// contradiction reports whether the discrete constraints admit no value.
func (d *discrete) contradiction() bool {
	if len(d.required) > 1 {
		return true // two distinct required equalities
	}
	if d.membership != nil {
		// The effective membership is the set minus the excluded values.
		effective := 0
		for v := range d.membership {
			if _, ex := d.excluded[v]; !ex {
				effective++
			}
		}
		if effective == 0 {
			return true // the `in` set is empty or fully excluded
		}
	}
	for v := range d.required {
		if _, ex := d.excluded[v]; ex {
			return true // the required value is excluded
		}
		if d.membership != nil {
			if _, in := d.membership[v]; !in {
				return true // the required value is outside the `in` set
			}
		}
	}
	return false
}

// litNumber extracts a float64 from a numeric literal (int/float forms), reporting
// false for a value the analyzer does not treat as numeric.
func litNumber(l state.Literal) (float64, bool) {
	switch v := l.Value.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	default:
		return 0, false
	}
}

// litKey is the comparable string key of a discrete literal (string/bool/enum).
func litKey(l state.Literal) string { return fmt.Sprintf("%v", l.Value) }
