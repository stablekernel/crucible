package state

import (
	"encoding/json"
	"sort"
)

// This file ships the registry palette — the discovery surface a visual builder
// reads to enumerate the host's registered behavior. The registry binds
// name -> implementation; the palette adds optional, serializable metadata
// (kind, description, a parameter schema, and read/write hints) so a UI can list
// every registered guard/action/service/actor and render a form for its params.
//
// The palette is purely additive: registration accepts an optional descriptor
// through a backward-compatible options tail, registering without one still
// works (it yields a minimal descriptor carrying just Kind and Name), and
// descriptors never affect binding, lint, or Fire semantics — they are metadata
// only. Everything here is stdlib-only and JSON-serializable, since the palette
// is meant to travel over a builder API.

// DescriptorKind names the category of a registered implementation in the
// palette: a guard predicate, an action/effect, an invoked service, or an actor
// behavior. It serializes as its lowercase string for a stable wire form.
type DescriptorKind string

// The descriptor kinds, one per palette-eligible registration surface.
const (
	// KindGuard marks a registered guard predicate.
	KindGuard DescriptorKind = "guard"
	// KindAction marks a registered action/effect.
	KindAction DescriptorKind = "action"
	// KindAssign marks a registered assign reducer — the sole context writer.
	KindAssign DescriptorKind = "assign"
	// KindService marks a registered invoked service.
	KindService DescriptorKind = "service"
	// KindActor marks a registered actor behavior.
	KindActor DescriptorKind = "actor"
)

// ParamType is the value type of a single ref parameter, used by a UI to pick
// the right form control. It is a minimal, stdlib-only set and serializes as its
// lowercase string so the schema travels cleanly over an API.
type ParamType string

// The parameter types. EnumParam additionally carries its allowed values on the
// owning ParamSpec via the Describe builder's EnumParamOf helper.
const (
	// StringParam is a free-form string.
	StringParam ParamType = "string"
	// IntParam is an integer.
	IntParam ParamType = "int"
	// FloatParam is a floating-point number.
	FloatParam ParamType = "float"
	// BoolParam is a boolean.
	BoolParam ParamType = "bool"
	// DurationParam is a time.Duration, conventionally carried as a Go duration
	// string (e.g. "1500ms").
	DurationParam ParamType = "duration"
	// EnumParam is a string constrained to an enumerated set; the allowed values
	// live on the ParamSpec.Enum field.
	EnumParam ParamType = "enum"
)

// ParamSpec describes one parameter a ref accepts: its name, type, whether it is
// required, an optional human description, an optional default value, and — for
// EnumParam — the allowed values. It JSON-serializes cleanly for transport to a
// builder UI that renders a form control from it.
type ParamSpec struct {
	Name        string    `json:"name"`
	Type        ParamType `json:"type"`
	Required    bool      `json:"required,omitempty"`
	Description string    `json:"description,omitempty"`
	Default     any       `json:"default,omitempty"`
	// Enum lists the allowed values when Type is EnumParam; it is empty for every
	// other type.
	Enum []string `json:"enum,omitempty"`
	// Examples lists sample values a UI can offer for this parameter — a
	// prefill or "did you mean" hint distinct from Default (the value used when
	// the parameter is omitted) and from Enum (the closed set of legal values).
	// It is empty unless the registration declares examples.
	Examples []any `json:"examples,omitempty"`
}

// Descriptor is the serializable palette entry for one registered implementation.
// It carries the implementation's kind and name (always present), an optional
// human description, the parameter schema a UI renders a form from, and optional
// context read/write hints naming the entity fields the implementation reads or
// writes. A registration with no Describe yields a minimal Descriptor with only
// Kind and Name set.
type Descriptor struct {
	Kind        DescriptorKind `json:"kind"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	// Category is an optional grouping label a UI uses to organize behaviors in
	// the palette (e.g. "guards", "side-effects", "lifecycle"). It is free-form
	// and absent unless the registration declares one.
	Category string `json:"category,omitempty"`
	// Examples lists optional example usages of the behavior as a whole — short
	// snippets a UI can show as documentation, distinct from ParamSpec.Examples
	// (which gives sample values for one parameter). It is empty unless the
	// registration declares examples.
	Examples []string    `json:"examples,omitempty"`
	Params   []ParamSpec `json:"params,omitempty"`
	// Reads and Writes are optional type hints naming the entity fields the
	// implementation reads from or writes to, for a UI that surfaces data flow.
	Reads  []string `json:"reads,omitempty"`
	Writes []string `json:"writes,omitempty"`
	// Binding is the reserved descriptor of how this named behavior is backed. It
	// is optional and absent by default; an absent binding means the behavior
	// resolves to the in-process Go registry entry (BindingTransportOf reads that
	// default). Reserving the slot now keeps a future out-of-process binding
	// (a sandboxed component, a remote service) an additive descriptor field rather
	// than a breaking change. The kernel never dispatches on it at v1.
	Binding *BindingSpec `json:"binding,omitempty"`
}

// TransportInProcess is the v1 default binding transport: the behavior is a Go
// func held in the host registry and called in-process. It is the only transport
// the kernel dispatches at v1; every other transport is reserved.
const TransportInProcess = "in-process"

// BindingSpec describes how a named behavior is backed. Transport names the
// invocation transport, defaulting to in-process when empty. Meta is a reserved
// per-binding extension namespace (e.g. a sandbox fuel budget, an endpoint).
//
// Transport follows the closed-enum extension policy: a transport this build does
// not recognize is preserved verbatim on round-trip (so a newer producer's binding
// survives an older client) and would be rejected only at dispatch — and no
// non-in-process dispatch path exists at v1. Unknown top-level keys are likewise
// preserved through extra.
type BindingSpec struct {
	Transport string         `json:"transport,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`

	// extra preserves unknown JSON keys a newer producer emitted so they survive a
	// load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// bindingSpecKnownKeys is the set of JSON keys BindingSpec models; anything else
// is captured into extra and preserved verbatim on round-trip.
var bindingSpecKnownKeys = map[string]struct{}{"transport": {}, "meta": {}}

// MarshalJSON encodes a BindingSpec, merging its preserved unknown keys back in
// with stable key ordering.
func (s BindingSpec) MarshalJSON() ([]byte, error) {
	type alias BindingSpec
	return marshalWithExtra(alias(s), s.extra)
}

// UnmarshalJSON decodes a BindingSpec and captures any unknown keys into extra so
// they survive re-serialization.
func (s *BindingSpec) UnmarshalJSON(data []byte) error {
	type alias BindingSpec
	var a alias
	extra, err := captureExtra(data, &a, bindingSpecKnownKeys)
	if err != nil {
		return err
	}
	*s = BindingSpec(a)
	s.extra = extra
	return nil
}

// transport returns the spec's transport, defaulting to in-process when unset.
func (s BindingSpec) transport() string {
	if s.Transport == "" {
		return TransportInProcess
	}
	return s.Transport
}

// BindingTransportOf returns the binding transport a descriptor declares,
// defaulting to in-process when the descriptor has no Binding (the common case) or
// an empty transport. It is the canonical reader of the reserved binding default.
func BindingTransportOf(d Descriptor) string {
	if d.Binding == nil {
		return TransportInProcess
	}
	return d.Binding.transport()
}

// describeSpec is the accumulated descriptor metadata built by Describe and its
// chained methods, applied to a registration's descriptor through the
// DescribeOption tail. It holds everything except Kind and Name, which the
// registration method supplies.
type describeSpec struct {
	description string
	category    string
	examples    []string
	params      []ParamSpec
	reads       []string
	writes      []string
}

// DescribeBuilder fluently accumulates a registration's descriptor metadata —
// its description, category, example usages, parameter schema, and read/write
// hints. Obtain one with Describe, chain Category / Examples / Param /
// OptionalParam / Reads / Writes, and pass it as the trailing option to a
// registration (Guard / Action / Service / Actor). A
// DescribeBuilder is itself a DescribeOption, so it drops straight into the
// options tail.
type DescribeBuilder struct {
	spec describeSpec
}

// Describe opens a fluent descriptor builder with the given human description.
// Chain Param / OptionalParam / Reads / Writes to declare the parameter schema
// and data-flow hints, then pass the builder as the trailing option to a
// registration:
//
//	reg.Guard("minAmount", minAmount,
//	    state.Describe("Passes when the amount is at least min.").
//	        Param("min", state.IntParam).
//	        OptionalParam("currency", state.StringParam).
//	        Reads("Order"))
func Describe(description string) *DescribeBuilder {
	return &DescribeBuilder{spec: describeSpec{description: description}}
}

// Param declares a required parameter of the given type.
func (d *DescribeBuilder) Param(name string, typ ParamType) *DescribeBuilder {
	d.spec.params = append(d.spec.params, ParamSpec{Name: name, Type: typ, Required: true})
	return d
}

// OptionalParam declares an optional parameter of the given type.
func (d *DescribeBuilder) OptionalParam(name string, typ ParamType) *DescribeBuilder {
	d.spec.params = append(d.spec.params, ParamSpec{Name: name, Type: typ})
	return d
}

// ParamSpec appends a fully-specified parameter, for cases needing a
// description, default, or enum values the shorthand Param/OptionalParam do not
// express.
func (d *DescribeBuilder) ParamSpec(p ParamSpec) *DescribeBuilder {
	d.spec.params = append(d.spec.params, p)
	return d
}

// EnumParam declares a required enum parameter constrained to the given allowed
// values.
func (d *DescribeBuilder) EnumParam(name string, allowed ...string) *DescribeBuilder {
	d.spec.params = append(d.spec.params, ParamSpec{
		Name:     name,
		Type:     EnumParam,
		Required: true,
		Enum:     append([]string(nil), allowed...),
	})
	return d
}

// Category sets the grouping label a UI uses to organize this behavior in the
// palette (e.g. "guards", "side-effects", "lifecycle"). The last call wins.
func (d *DescribeBuilder) Category(category string) *DescribeBuilder {
	d.spec.category = category
	return d
}

// Examples records example usages of the behavior as a whole — short snippets a
// UI can show as documentation. Successive calls accumulate. For sample values of
// a single parameter, set ParamSpec.Examples via ParamSpec instead.
func (d *DescribeBuilder) Examples(examples ...string) *DescribeBuilder {
	d.spec.examples = append(d.spec.examples, examples...)
	return d
}

// Reads records the entity fields the implementation reads, a data-flow hint for
// a UI. Successive calls accumulate.
func (d *DescribeBuilder) Reads(fields ...string) *DescribeBuilder {
	d.spec.reads = append(d.spec.reads, fields...)
	return d
}

// Writes records the entity fields the implementation writes, a data-flow hint
// for a UI. Successive calls accumulate.
func (d *DescribeBuilder) Writes(fields ...string) *DescribeBuilder {
	d.spec.writes = append(d.spec.writes, fields...)
	return d
}

// apply makes DescribeBuilder satisfy DescribeOption, so the builder itself is
// the option passed in a registration's tail.
func (d *DescribeBuilder) apply(s *describeSpec) { *s = d.spec }

// DescribeOption configures the optional descriptor attached to a registration.
// A *DescribeBuilder is the canonical implementation; the option tail keeps
// registration backward-compatible — calling Guard/Action/Service/Actor with no
// option still works and yields a minimal descriptor.
type DescribeOption interface {
	apply(*describeSpec)
}

// resolveDescribe folds the option tail into a describeSpec, returning the
// accumulated spec and whether any option was supplied. Only the last option is
// applied when several are passed; the fluent builder is meant to be used as a
// single trailing option.
func resolveDescribe(opts []DescribeOption) (describeSpec, bool) {
	var spec describeSpec
	if len(opts) == 0 {
		return spec, false
	}
	for _, o := range opts {
		if o != nil {
			o.apply(&spec)
		}
	}
	return spec, true
}

// descriptorFrom assembles a Descriptor from a kind, name, and accumulated spec.
func descriptorFrom(kind DescriptorKind, name string, spec describeSpec) Descriptor {
	return Descriptor{
		Kind:        kind,
		Name:        name,
		Description: spec.description,
		Category:    spec.category,
		Examples:    append([]string(nil), spec.examples...),
		Params:      append([]ParamSpec(nil), spec.params...),
		Reads:       append([]string(nil), spec.reads...),
		Writes:      append([]string(nil), spec.writes...),
	}
}

// Palette returns a descriptor for every consumer-registered guard, action,
// service, and actor behavior in the registry, sorted deterministically by kind
// then name. Entries registered without a Describe descriptor still appear,
// carrying a minimal descriptor with just Kind and Name. Built-in actions
// (spawn/cancel/send/raise) and the stateIn guard are language-level, not
// registered, and are intentionally excluded; BuiltinPalette lists those.
//
// The returned slice is freshly allocated each call and safe for the caller to
// retain or mutate.
func (r *Registry[C]) Palette() []Descriptor {
	out := make([]Descriptor, 0, len(r.guards)+len(r.actions)+len(r.assigns)+len(r.services)+len(r.actorDescs))

	collect := func(kind DescriptorKind, names []string) {
		for _, name := range names {
			if d, ok := r.descriptors[descriptorKey(kind, name)]; ok {
				out = append(out, d)
				continue
			}
			out = append(out, Descriptor{Kind: kind, Name: name})
		}
	}

	collect(KindGuard, keysOf(r.guards))
	collect(KindAction, keysOf(r.actions))
	collect(KindAssign, keysOf(r.assigns))
	collect(KindService, keysOf(r.services))
	collect(KindActor, r.actorDescs)

	sortDescriptors(out)
	return out
}

// sortDescriptors orders descriptors deterministically: by kind, then by name.
func sortDescriptors(d []Descriptor) {
	sort.Slice(d, func(i, j int) bool {
		if d[i].Kind != d[j].Kind {
			return d[i].Kind < d[j].Kind
		}
		return d[i].Name < d[j].Name
	})
}

// keysOf returns the keys of a name-keyed map as an unsorted slice; Palette
// sorts the assembled descriptors, so the intermediate order does not matter.
func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// descriptorKey namespaces a descriptor by kind and name so guards, actions,
// services, and actors never collide on a shared name.
func descriptorKey(kind DescriptorKind, name string) string {
	return string(kind) + "\x00" + name
}

// BuiltinPalette returns descriptors for the language-level built-ins the kernel
// recognizes without host registration — the actor and scheduling actions
// (spawn, stop-actor/stop-child, send/forward/respond, send-parent, cancel) and
// the stateIn guard. They are excluded from Palette because they are part of the
// language, not the host's registry; a builder lists them from this fixed set so
// the editor surfaces the full vocabulary. The returned slice is freshly
// allocated and sorted deterministically.
func BuiltinPalette() []Descriptor {
	out := []Descriptor{
		{
			Kind:        KindGuard,
			Name:        string(GuardStateIn),
			Description: "Passes when the instance is currently in the named state.",
			Params:      []ParamSpec{{Name: "state", Type: StringParam, Required: true}},
		},
		{
			Kind:        KindAction,
			Name:        spawnBuiltinName,
			Description: "Spawns a child-machine actor.",
			Params: []ParamSpec{
				{Name: spawnSrcParam, Type: StringParam, Required: true},
				{Name: spawnIDParam, Type: StringParam, Required: true},
			},
		},
		{
			Kind:        KindAction,
			Name:        stopActorBuiltinName,
			Description: "Stops a running spawned or invoked-child actor by id.",
			Params:      []ParamSpec{{Name: stopActorIDParam, Type: StringParam, Required: true}},
		},
		{
			Kind:        KindAction,
			Name:        sendToBuiltinName,
			Description: "Sends an event to the actor registered under the target id.",
			Params:      []ParamSpec{{Name: sendToTargetParam, Type: StringParam}},
		},
		{
			Kind:        KindAction,
			Name:        sendParentBuiltinName,
			Description: "Sends an event to the emitting actor's parent.",
		},
		{
			Kind:        KindAction,
			Name:        respondBuiltinName,
			Description: "Replies to the sender of the event being handled.",
		},
		{
			Kind:        KindAction,
			Name:        forwardToBuiltinName,
			Description: "Forwards the current event to another actor.",
			Params:      []ParamSpec{{Name: sendToTargetParam, Type: StringParam}},
		},
		{
			Kind:        KindAction,
			Name:        cancelBuiltinName,
			Description: "Cancels a pending delayed (after) event by schedule id.",
			Params:      []ParamSpec{{Name: cancelIDParam, Type: StringParam, Required: true}},
		},
	}
	sortDescriptors(out)
	return out
}
