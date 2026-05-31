package expr

import (
	"encoding/base64"
	"fmt"
	"sort"
)

// MetaKey is the reserved machine-level Meta key under which a rich-guard sidecar
// travels in the IR. The kernel never reads it; it round-trips verbatim through the
// IR envelope's Meta like any other extension namespace, carrying the type-checked
// CEL ASTs that analysis and polyglot tooling consume.
const MetaKey = "crucible.expr/rich"

// Dialect names the expression dialect a rich entry is authored in. Only CEL ships
// in v1; the field exists so the sidecar can carry a second dialect additively.
const Dialect = "cel"

// RichEntry is one rich guard's serializable sidecar record: its source text (for
// round-trip and edit), its dialect, and the type-checked AST as canonical
// cel.dev/expr CheckedExpr bytes (the form a polyglot evaluator consumes). It is
// the data half of a rich guard — the behavior half is the binding registered under
// the same name.
type RichEntry struct {
	// Source is the original CEL source text the guard was authored from.
	Source string `json:"source"`
	// Dialect names the expression language; "cel" in v1.
	Dialect string `json:"dialect"`
	// CheckedAST is the type-checked AST serialized as canonical cel.dev/expr
	// CheckedExpr proto bytes.
	CheckedAST []byte `json:"checkedAST"`
}

// Catalog is the name-keyed sidecar collector a host passes to Guard so each rich
// guard's source and type-checked AST are recorded as the guard is authored. After
// authoring, Meta attaches the catalog to an IR's machine-level Meta and LoadCatalog
// reads it back, so the rich ASTs travel with the machine through a JSON round-trip
// without the kernel ever inspecting them. The zero Catalog is not usable; build one
// with NewCatalog.
type Catalog struct {
	entries map[string]RichEntry
}

// NewCatalog returns an empty rich-guard catalog ready to collect entries.
func NewCatalog() *Catalog {
	return &Catalog{entries: map[string]RichEntry{}}
}

// add records a rich entry under name, rejecting a duplicate so two guards never
// silently collide on a shared name in the sidecar.
func (c *Catalog) add(name string, e RichEntry) error {
	if _, exists := c.entries[name]; exists {
		return fmt.Errorf("rich guard %q already in catalog", name)
	}
	c.entries[name] = e
	return nil
}

// Names returns the catalog's guard names in sorted order, so tooling can enumerate
// the rich guards a machine declares deterministically.
func (c *Catalog) Names() []string {
	out := make([]string, 0, len(c.entries))
	for name := range c.entries {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Entry returns the rich entry recorded under name and whether it exists.
func (c *Catalog) Entry(name string) (RichEntry, bool) {
	e, ok := c.entries[name]
	return e, ok
}

// Len reports how many rich guards the catalog holds.
func (c *Catalog) Len() int { return len(c.entries) }

// Meta renders the catalog as the machine-level Meta value a host stores under
// MetaKey in the IR. The CheckedAST bytes are base64-encoded so the sidecar is a
// pure JSON value that survives the IR's encoding/json round-trip unchanged. A host
// merges the returned single-entry map into the IR's Meta.
func (c *Catalog) Meta() map[string]any {
	guards := make(map[string]any, len(c.entries))
	for name, e := range c.entries {
		guards[name] = map[string]any{
			"source":     e.Source,
			"dialect":    e.Dialect,
			"checkedAST": base64.StdEncoding.EncodeToString(e.CheckedAST),
		}
	}
	return map[string]any{MetaKey: guards}
}

// LoadCatalog reconstructs a Catalog from an IR's machine-level Meta, reading the
// sidecar stored under MetaKey. A Meta with no rich sidecar yields an empty catalog
// and no error, so a machine that never used the rich tier loads cleanly. A
// malformed sidecar entry is reported so tampering or a version skew fails loudly.
func LoadCatalog(meta map[string]any) (*Catalog, error) {
	cat := NewCatalog()
	if meta == nil {
		return cat, nil
	}
	raw, ok := meta[MetaKey]
	if !ok {
		return cat, nil
	}
	guards, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("rich sidecar under %q is %T, want object", MetaKey, raw)
	}
	for name, ev := range guards {
		entry, err := decodeEntry(ev)
		if err != nil {
			return nil, fmt.Errorf("rich guard %q: %w", name, err)
		}
		cat.entries[name] = entry
	}
	return cat, nil
}

// decodeEntry decodes one rich sidecar entry from its JSON-decoded map form,
// base64-decoding the checked-AST bytes.
func decodeEntry(v any) (RichEntry, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return RichEntry{}, fmt.Errorf("entry is %T, want object", v)
	}
	source, _ := m["source"].(string)
	dialect, _ := m["dialect"].(string)
	encoded, _ := m["checkedAST"].(string)
	astBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return RichEntry{}, fmt.Errorf("decode checkedAST: %w", err)
	}
	return RichEntry{Source: source, Dialect: dialect, CheckedAST: astBytes}, nil
}
