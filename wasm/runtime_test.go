package wasm_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stablekernel/crucible/wasm"
	"github.com/tetratelabs/wazero"
)

// guardWASM, badWASM, and loopWASM hold the compiled guests, built once in TestMain.
var (
	guardWASM []byte
	badWASM   []byte
	loopWASM  []byte
)

// TestMain compiles the testdata guests to wasip1/wasm with the standard Go
// toolchain (no TinyGo, no committed binary) so the tests run them through wazero.
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "wasmguest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmp)

	guardWASM = buildGuest(tmp, "guard", "./testdata/guardguest")
	badWASM = buildGuest(tmp, "bad", "./testdata/badguest")
	loopWASM = buildGuest(tmp, "loop", "./testdata/loopguest")
	os.Exit(m.Run())
}

// buildGuest compiles a wasip1 guest reactor module and returns its bytes.
// -buildmode=c-shared yields a reactor: package init runs in _initialize and the
// //go:wasmexport functions stay callable, rather than a command whose _start runs
// main and exits, leaving the exports uncallable.
func buildGuest(dir, name, pkg string) []byte {
	out := filepath.Join(dir, name+".wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, pkg)
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if buildOut, err := cmd.CombinedOutput(); err != nil {
		panic("build guest " + pkg + ": " + err.Error() + "\n" + string(buildOut))
	}
	b, err := os.ReadFile(out)
	if err != nil {
		panic(err)
	}
	return b
}

// TestModule_EvalRejectsOutOfRangeResponse confirms the host rejects a guest that
// returns a response pointer outside linear memory rather than reading out of bounds.
func TestModule_EvalRejectsOutOfRangeResponse(t *testing.T) {
	ctx := context.Background()
	mod, err := wasm.Compile(ctx, badWASM)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(ctx) })
	if _, err := mod.Eval(ctx, []byte(`{}`)); err == nil {
		t.Fatal("Eval against an out-of-range response = nil error, want a bounds error")
	}
}

// TestModule_EvalTimesOutRunawayGuest confirms a guest that never returns is
// interrupted when the call context's deadline elapses, so the host is not blocked
// forever. This exercises the runtime's WithCloseOnContextDone(true) wiring.
func TestModule_EvalTimesOutRunawayGuest(t *testing.T) {
	mod, err := wasm.Compile(context.Background(), loopWASM)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, evalErr := mod.Eval(ctx, []byte(`{}`))
		done <- evalErr
	}()

	select {
	case evalErr := <-done:
		if evalErr == nil {
			t.Fatal("Eval of a runaway guest = nil error, want a context/closed error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Eval of a runaway guest did not return after the context deadline: the runtime did not interrupt the guest")
	}
}

// TestCompile_WithRuntimeConfig confirms Compile accepts a custom RuntimeConfig
// option and still produces a working module. The option is additive: omitting it
// uses the default (which closes a guest on context-done).
func TestCompile_WithRuntimeConfig(t *testing.T) {
	ctx := context.Background()
	mod, err := wasm.Compile(ctx, guardWASM,
		wasm.WithRuntimeConfig(wazero.NewRuntimeConfig().WithCloseOnContextDone(true)))
	if err != nil {
		t.Fatalf("compile with runtime config: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(ctx) })

	resp, err := mod.Eval(ctx, []byte(`{"context":{"status":"paid","total":50}}`))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if string(resp) != `{"ok":true}` {
		t.Fatalf("response = %s, want {\"ok\":true}", resp)
	}
}

// TestModule_EvalRoundTrip is the ABI proof: a JSON request crosses into the guest,
// the guest evaluates it, and the JSON response crosses back.
func TestModule_EvalRoundTrip(t *testing.T) {
	ctx := context.Background()
	mod, err := wasm.Compile(ctx, guardWASM)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(ctx) })

	cases := []struct {
		name string
		req  string
		want string
	}{
		{"approved", `{"context":{"status":"paid","total":50}}`, `{"ok":true}`},
		{"too-small", `{"context":{"status":"paid","total":10}}`, `{"ok":false}`},
		{"unpaid", `{"context":{"status":"open","total":99}}`, `{"ok":false}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := mod.Eval(ctx, []byte(tc.req))
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if string(resp) != tc.want {
				t.Fatalf("response = %s, want %s", resp, tc.want)
			}
		})
	}
}
