package state

import (
	"encoding/json"
	"strconv"
	"strings"
)

// CurrentSchemaVersion is the IR wire-format version this build emits and
// accepts. It is a major.minor string: a higher minor (same major) loads with
// unknown fields preserved for forward-compat, while a higher major is refused by
// LoadFromJSON as *UnsupportedSchemaError. Every document ToJSON emits is stamped
// with this version, so an IR on the wire is self-describing.
const CurrentSchemaVersion = "1.0"

// IOSpec is the reserved declaration slot for a machine's input or done-output
// shape. At v1 it is opaque: Schema is a free-form, namespace-reserved
// description of the shape and Description is human documentation. A later
// data-model/typing module can give Schema teeth without changing the wire field.
// Meta is the per-spec extension namespace, round-tripped verbatim like every
// other Meta in the IR.
type IOSpec struct {
	// Schema is an opaque declaration of the input/output shape. The kernel never
	// inspects it; it travels for tooling and a future typing layer.
	Schema map[string]any `json:"schema,omitempty"`
	// Description is human-readable documentation of the slot.
	Description string `json:"description,omitempty"`
	// Meta is the reserved extension namespace for this spec.
	Meta map[string]any `json:"meta,omitempty"`

	// extra preserves unknown JSON keys a newer producer emitted so they survive a
	// load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// ioSpecKnownKeys is the set of JSON keys IOSpec models; anything else is captured
// into extra and preserved verbatim on round-trip.
var ioSpecKnownKeys = map[string]struct{}{
	"schema": {}, "description": {}, "meta": {},
}

// MarshalJSON encodes an IOSpec, merging its preserved unknown keys back in with
// stable key ordering.
func (s IOSpec) MarshalJSON() ([]byte, error) {
	type alias IOSpec
	return marshalWithExtra(alias(s), s.extra)
}

// UnmarshalJSON decodes an IOSpec and captures any unknown keys into extra so they
// survive re-serialization.
func (s *IOSpec) UnmarshalJSON(data []byte) error {
	type alias IOSpec
	var a alias
	extra, err := captureExtra(data, &a, ioSpecKnownKeys)
	if err != nil {
		return err
	}
	*s = IOSpec(a)
	s.extra = extra
	return nil
}

// schemaVersionAtLeastMajor reports whether got declares a major version greater
// than the loader's. An empty or unparseable major is treated as compatible
// (major 0 / legacy), so pre-versioned documents continue to load. Only the major
// component gates acceptance; minor differences are forward-compat and preserved.
func schemaMajorRejected(got string) bool {
	gotMajor, ok := parseMajor(got)
	if !ok {
		// Absent or malformed version: a legacy / pre-envelope document. Accept it;
		// preserve-unknown keeps any fields a newer producer added.
		return false
	}
	supMajor, _ := parseMajor(CurrentSchemaVersion)
	return gotMajor > supMajor
}

// parseMajor extracts the leading integer major component of a major[.minor...]
// version string. It reports ok=false for an empty string or a non-numeric major,
// letting the caller treat such documents as legacy/compatible.
func parseMajor(v string) (int, bool) {
	if v == "" {
		return 0, false
	}
	major := v
	if i := strings.IndexByte(v, '.'); i >= 0 {
		major = v[:i]
	}
	n, err := strconv.Atoi(major)
	if err != nil {
		return 0, false
	}
	return n, true
}

// marshalWithExtra encodes a value's known fields together with a verbatim map of
// preserved unknown keys, emitting a single JSON object with keys in sorted order.
// known is marshaled normally (honoring its json tags and omitempty), the result
// is reduced to its key/value pairs, the extras are merged in (known keys win),
// and the union is re-encoded via a map so encoding/json sorts the keys — yielding
// byte-stable output regardless of map iteration order. This is the mechanism that
// makes unknown JSON fields survive a load -> save cycle (forward-compat) while
// keeping the encoding canonical for golden diffing and future digesting.
func marshalWithExtra(known any, extra map[string]json.RawMessage) ([]byte, error) {
	b, err := json.Marshal(known)
	if err != nil {
		return nil, err
	}
	if len(extra) == 0 {
		return b, nil
	}
	merged := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		merged[k] = v
	}
	// Decode known fields last so a real field always shadows a stale extra of the
	// same name (extras only ever hold keys the struct does not model, but this
	// keeps the contract explicit).
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, err
	}
	for k, v := range fields {
		merged[k] = v
	}
	return json.Marshal(merged)
}

// captureExtra unmarshals data into the known-field target and returns the raw
// JSON of every top-level key the target does not model, so unknown fields are
// preserved verbatim for re-emission. knownKeys is the set of JSON keys the
// target struct owns; any key outside it is captured. A nil result means there
// were no unknown keys.
func captureExtra(data []byte, knownTarget any, knownKeys map[string]struct{}) (map[string]json.RawMessage, error) {
	if err := json.Unmarshal(data, knownTarget); err != nil {
		return nil, err
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		// data was valid for the typed target but is not a JSON object (e.g. null):
		// there are no extra keys to capture.
		return nil, nil //nolint:nilerr // a non-object body simply carries no extras.
	}
	var extra map[string]json.RawMessage
	for k, v := range all {
		if _, known := knownKeys[k]; known {
			continue
		}
		if extra == nil {
			extra = make(map[string]json.RawMessage)
		}
		extra[k] = v
	}
	return extra, nil
}

// cloneRawExtra deep-copies a preserved-unknown map so a deep-copied IR node never
// aliases the source's raw extras. The json.RawMessage byte slices are copied so a
// later in-place mutation of one cannot leak into the other.
func cloneRawExtra(in map[string]json.RawMessage) map[string]json.RawMessage {
	if in == nil {
		return nil
	}
	out := make(map[string]json.RawMessage, len(in))
	for k, v := range in {
		cp := make(json.RawMessage, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

// cloneMeta deep-copies a Meta extension map one level deep, enough that mutating
// the copy's top-level entries never touches the source. Values are any; nested
// maps/slices are shared by reference, matching the verbatim round-trip contract
// (the kernel never mutates Meta values, only carries them).
func cloneMeta(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// schemaError builds the typed rejection for a document whose schema major
// exceeds the loader's. Centralized so the message stays consistent.
func schemaError(got string) error {
	return &UnsupportedSchemaError{Got: got, Supported: CurrentSchemaVersion}
}
