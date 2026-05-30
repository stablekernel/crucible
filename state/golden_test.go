package state_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// updateGolden rewrites the committed IR golden files when set, so a deliberate
// machine change refreshes the goldens in one reviewable diff.
var updateGolden = flag.Bool("update-golden", false, "rewrite golden IR files")

// goldenIR holds one machine whose serialized IR is pinned as a golden file.
type goldenIR struct {
	name string
	json func() ([]byte, error)
}

func goldenMachines() []goldenIR {
	return []goldenIR{
		// Serialize WithoutSrcPos so the goldens carry no absolute source paths
		// and stay byte-identical across checkouts and CI. Source positions are
		// diagnostic-only and would otherwise pin the goldens to the authoring
		// worktree's filesystem path.
		{"document", func() ([]byte, error) { return buildDocMachine().ToJSON(state.WithoutSrcPos()) }},
		{"job", func() ([]byte, error) { return buildJobMachine().ToJSON(state.WithoutSrcPos()) }},
		{"worker", func() ([]byte, error) { return buildWorkerMachine().ToJSON(state.WithoutSrcPos()) }},
	}
}

// TestGoldenIR diffs each machine's serialized IR against its committed golden
// file. The IR is canonical, so a structural change to a machine is a visible
// diff in CI. Run with -update-golden to refresh after an intended change.
//
// The Mermaid and DOT renderings of these same machines are pinned alongside
// the IR goldens by TestGoldenViz, which reuses these example machines and the
// shared -update-golden flag.
func TestGoldenIR(t *testing.T) {
	dir := filepath.Join("testdata", "ir")
	if *updateGolden {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	for _, g := range goldenMachines() {
		t.Run(g.name, func(t *testing.T) {
			raw, err := g.json()
			if err != nil {
				t.Fatalf("ToJSON: %v", err)
			}
			// Indent for a human-readable, line-diffable golden.
			var pretty bytes.Buffer
			if indentErr := json.Indent(&pretty, raw, "", "  "); indentErr != nil {
				t.Fatalf("indent: %v", indentErr)
			}
			pretty.WriteByte('\n')
			path := filepath.Join(dir, g.name+".json")

			if *updateGolden {
				if writeErr := os.WriteFile(path, pretty.Bytes(), 0o644); writeErr != nil {
					t.Fatalf("write golden: %v", writeErr)
				}
				return
			}

			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run -update-golden to create): %v", err)
			}
			if !bytes.Equal(want, pretty.Bytes()) {
				t.Errorf("IR golden mismatch for %s; run with -update-golden if intended", g.name)
			}
		})
	}
}

// TestGoldenIR_RoundTrips asserts each golden IR reloads and reserializes to the
// same canonical bytes — the structural half of round-trip identity, pinned.
func TestGoldenIR_RoundTrips(t *testing.T) {
	first, err := buildDocMachine().ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](first)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	second, err := ir.Provide(docRegistry()).Quench().ToJSON()
	if err != nil {
		t.Fatalf("reserialize: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("IR not byte-stable under round-trip")
	}
}
