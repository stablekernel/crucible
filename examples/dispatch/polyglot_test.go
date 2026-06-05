package dispatch

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
)

// buildGenerousGuest compiles the generous-order WebAssembly guest to wasip1/wasm with
// the standard Go toolchain (no TinyGo, no committed binary) and returns its bytes. The
// -buildmode=c-shared flag yields a reactor: package init runs in _initialize and the
// //go:wasmexport functions stay callable, rather than a command whose _start runs main
// and exits, leaving the exports uncallable. It mirrors the wasm package's own guest
// build so the dispatch showcase compiles its guard the same proven way.
func buildGenerousGuest(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "generous.wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, "./testdata/generousguest")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if buildOut, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build generous guest: %v\n%s", err, buildOut)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read built guest: %v", err)
	}
	return b
}

// TestRunPolyglotEquivalence_CELAndWASMAgree compiles the generous-order guard to
// WebAssembly, drives both the default CEL-guarded order model and the WASM-guarded
// model through the Authorized decision, and asserts the two engines reach identical
// outcomes for every order — including at least one admit and one reject, so the
// agreement is non-vacuous. It is the proof that the saga's generous guard is polyglot:
// behaviorally identical whether evaluated in CEL or in WebAssembly.
func TestRunPolyglotEquivalence_CELAndWASMAgree(t *testing.T) {
	ctx := context.Background()
	report, err := RunPolyglotEquivalence(ctx, buildGenerousGuest(t))
	if err != nil {
		t.Fatalf("RunPolyglotEquivalence: %v", err)
	}

	if !report.Equivalent {
		t.Fatalf("CEL and WASM guards should be equivalent; report=%+v", report)
	}
	if len(report.Cases) != 2 {
		t.Fatalf("expected 2 isolating cases, got %d", len(report.Cases))
	}

	var sawAdmit, sawReject bool
	for _, c := range report.Cases {
		if !c.Agree {
			t.Fatalf("case %q disagreed: CEL=%v WASM=%v", c.Name, c.CEL, c.WASM)
		}
		if c.CEL != c.WASM {
			t.Fatalf("case %q outcomes differ: CEL=%v WASM=%v", c.Name, c.CEL, c.WASM)
		}
		switch c.Name {
		case "generous":
			if c.CEL != outcomeAdmitted {
				t.Fatalf("generous order should be admitted by both engines; got %v", c.CEL)
			}
			sawAdmit = true
		case "frugal":
			if c.CEL != outcomeBlocked {
				t.Fatalf("frugal order should be blocked by both engines; got %v", c.CEL)
			}
			sawReject = true
		}
	}
	if !sawAdmit || !sawReject {
		t.Fatalf("equivalence must exercise both verdicts; sawAdmit=%v sawReject=%v", sawAdmit, sawReject)
	}
}

// TestRunPolyglotEquivalence_RejectsBadModule confirms the harness surfaces a compile
// failure rather than swallowing it: non-WASM bytes can never back a guard, so the
// equivalence run must error.
func TestRunPolyglotEquivalence_RejectsBadModule(t *testing.T) {
	if _, err := RunPolyglotEquivalence(context.Background(), []byte("not wasm")); err == nil {
		t.Fatal("RunPolyglotEquivalence with non-wasm bytes = nil error, want a compile failure")
	}
}

// TestRunPolyglotEquivalence_RejectsModuleWithoutABI confirms a structurally valid but
// ABI-less module (just the WebAssembly header) fails to compile into a guard, so the
// harness errors rather than producing a meaningless report.
func TestRunPolyglotEquivalence_RejectsModuleWithoutABI(t *testing.T) {
	// The smallest valid WebAssembly module: the 8-byte header (magic + version), which
	// lacks the alloc/eval exports a guard module needs.
	empty := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	if _, err := RunPolyglotEquivalence(context.Background(), empty); err == nil {
		t.Fatal("RunPolyglotEquivalence with an ABI-less module = nil error, want a failure")
	}
}

// TestRunPolyglotEquivalence_ModelBuildErrors confirms a model-construction failure —
// whether building the CEL model or the WASM model — surfaces through the harness rather
// than being swallowed. The model builder seam is injected to fail.
func TestRunPolyglotEquivalence_ModelBuildErrors(t *testing.T) {
	t.Parallel()
	wasmBytes := buildGenerousGuest(t)
	sentinel := errors.New("model build boom")

	t.Run("CEL model build fails", func(t *testing.T) {
		t.Parallel()
		deps := productionDeps()
		deps.newModel = func(...fooddelivery.Option) (*state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], error) {
			return nil, sentinel
		}
		if _, err := runPolyglotEquivalence(context.Background(), wasmBytes, deps); !errors.Is(err, sentinel) {
			t.Fatalf("expected the model build error to surface; got %v", err)
		}
	})

	t.Run("WASM model build fails", func(t *testing.T) {
		t.Parallel()
		real := productionDeps()
		calls := 0
		deps := real
		deps.newModel = func(opts ...fooddelivery.Option) (*state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], error) {
			calls++
			if calls == 1 { // the CEL model builds; the WASM model (second call) fails.
				return real.newModel()
			}
			return nil, sentinel
		}
		if _, err := runPolyglotEquivalence(context.Background(), wasmBytes, deps); !errors.Is(err, sentinel) {
			t.Fatalf("expected the WASM model build error to surface; got %v", err)
		}
	})
}

// TestRunPolyglotEquivalence_DriveErrors confirms a drive failure on either engine
// surfaces through the harness, exercising both per-case error paths via the drive seam.
func TestRunPolyglotEquivalence_DriveErrors(t *testing.T) {
	t.Parallel()
	wasmBytes := buildGenerousGuest(t)
	sentinel := errors.New("drive boom")

	t.Run("CEL drive fails", func(t *testing.T) {
		t.Parallel()
		deps := productionDeps()
		deps.driveAuthorize = func(context.Context, *state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], fooddelivery.Order) (orderOutcome, error) {
			return outcomeBlocked, sentinel
		}
		if _, err := runPolyglotEquivalence(context.Background(), wasmBytes, deps); !errors.Is(err, sentinel) {
			t.Fatalf("expected the CEL drive error to surface; got %v", err)
		}
	})

	t.Run("WASM drive fails", func(t *testing.T) {
		t.Parallel()
		real := productionDeps()
		calls := 0
		deps := real
		deps.driveAuthorize = func(ctx context.Context, m *state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], o fooddelivery.Order) (orderOutcome, error) {
			calls++
			if calls == 1 { // the CEL drive succeeds; the WASM drive (second call) fails.
				return real.driveAuthorize(ctx, m, o)
			}
			return outcomeBlocked, sentinel
		}
		if _, err := runPolyglotEquivalence(context.Background(), wasmBytes, deps); !errors.Is(err, sentinel) {
			t.Fatalf("expected the WASM drive error to surface; got %v", err)
		}
	})
}

// TestDriveAuthorized_BlocksSubThreshold covers driveAuthorized's blocked return: a
// sub-threshold, non-fast-lane order is settled through authorize but the generous guard
// (and the Core branch) reject it, so it rests in Authorizing rather than reaching
// Cooking — the non-admitted outcome.
func TestDriveAuthorized_BlocksSubThreshold(t *testing.T) {
	m, err := fooddelivery.NewModel()
	if err != nil {
		t.Fatalf("NewModel: %v", err)
	}
	out, err := driveAuthorized(context.Background(), m, fooddelivery.Order{Subtotal: 1000, Tip: 100, Priority: "standard"})
	if err != nil {
		t.Fatalf("driveAuthorized: %v", err)
	}
	if out != outcomeBlocked {
		t.Fatalf("sub-threshold order should be blocked; got %v", out)
	}
}

// TestOrderOutcome_String covers the symbolic rendering of both outcomes.
func TestOrderOutcome_String(t *testing.T) {
	if got := outcomeAdmitted.String(); got != "admitted" {
		t.Fatalf("outcomeAdmitted.String() = %q, want admitted", got)
	}
	if got := outcomeBlocked.String(); got != "blocked" {
		t.Fatalf("outcomeBlocked.String() = %q, want blocked", got)
	}
}
