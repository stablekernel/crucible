package dispatch_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/stablekernel/crucible/examples/dispatch"
)

// buildGenerousGuestForExample compiles the generous-order WebAssembly guest to
// wasip1/wasm and returns its bytes, panicking on failure (an Example has no *testing.T
// to fail). It mirrors the test helper's c-shared reactor build so the Example exercises
// the same real WebAssembly guard the tests do.
func buildGenerousGuestForExample() []byte {
	dir, err := os.MkdirTemp("", "generousguest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	out := filepath.Join(dir, "generous.wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, "./testdata/generousguest")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if buildOut, berr := cmd.CombinedOutput(); berr != nil {
		panic("build generous guest: " + berr.Error() + "\n" + string(buildOut))
	}
	b, err := os.ReadFile(out)
	if err != nil {
		panic(err)
	}
	return b
}

// ExampleRunPolyglotEquivalence runs the generous-order guard in both the in-tree CEL
// engine and a WebAssembly guest, then prints whether the two engines decided every
// order identically — the polyglot-guard proof. The generous order is admitted by both
// engines; the frugal order is blocked by both; and because the suite exercises both
// verdicts, the agreement is meaningful.
func ExampleRunPolyglotEquivalence() {
	report, err := dispatch.RunPolyglotEquivalence(context.Background(), buildGenerousGuestForExample())
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	for _, c := range report.Cases {
		fmt.Printf("%s: CEL=%v WASM=%v agree=%v\n", c.Name, c.CEL, c.WASM, c.Agree)
	}
	fmt.Println("equivalent:", report.Equivalent)
	// Output:
	// generous: CEL=admitted WASM=admitted agree=true
	// frugal: CEL=blocked WASM=blocked agree=true
	// equivalent: true
}
