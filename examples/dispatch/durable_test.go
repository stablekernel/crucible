package dispatch_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/examples/dispatch"
	"github.com/stablekernel/crucible/examples/fooddelivery"
)

// TestRunCrashRecovery_SurvivesCrashAndDelivers drives the order saga to its live
// Active configuration under an on-disk FileStore, simulates a crash, recovers the
// order from the store alone, and drives it on to Delivered. It asserts the live
// state, the payment hold, and the folded log all survived the crash, and that the
// recovered order completes — the durable-execution guarantee made observable.
func TestRunCrashRecovery_SurvivesCrashAndDelivers(t *testing.T) {
	ctx := context.Background()
	result, err := dispatch.RunCrashRecovery(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("RunCrashRecovery: %v", err)
	}

	assertConfig(t, "recovered", result.RecoveredConfig, fooddelivery.Cooking, fooddelivery.OnTime)
	if result.RecoveredAuthHold == "" {
		t.Fatal("recovered order lost its payment authorization hold across the crash")
	}
	if !logHas(result.RecoveredLog, "authorized:"+result.RecoveredAuthHold) {
		t.Fatalf("recovered log %v missing the authorized milestone for hold %q",
			result.RecoveredLog, result.RecoveredAuthHold)
	}

	assertConfig(t, "final", result.FinalConfig, fooddelivery.Delivered)
	for _, want := range []string{"kitchen:prepared-meal", "courier:drop-confirmed", "captured"} {
		if !logHas(result.FinalLog, want) {
			t.Fatalf("final log %v missing %q", result.FinalLog, want)
		}
	}
}

// TestRunCrashRecovery_OpenError surfaces a store-open failure as an error rather
// than a panic, covering the harness's error path: a file path that is not a usable
// directory cannot back a FileStore.
func TestRunCrashRecovery_OpenError(t *testing.T) {
	ctx := context.Background()
	// A path under a file (not a directory) cannot be created as a store directory.
	bad := t.TempDir() + "/not-a-dir/\x00invalid"
	if _, err := dispatch.RunCrashRecovery(ctx, bad); err == nil {
		t.Fatal("expected an error opening a store at an invalid path, got nil")
	}
}

// TestRunTimeTravel_ReconstructsEarlierState records the saga's happy path through a
// history-retaining MemStore and reconstructs the order's state at an earlier step,
// asserting it differs from the delivered terminal — the read-only time-travel
// guarantee made observable.
func TestRunTimeTravel_ReconstructsEarlierState(t *testing.T) {
	ctx := context.Background()
	result, err := dispatch.RunTimeTravel(ctx)
	if err != nil {
		t.Fatalf("RunTimeTravel: %v", err)
	}

	if len(result.Timeline) == 0 {
		t.Fatal("time-travel timeline is empty")
	}
	assertConfig(t, "earlier", result.EarlierConfig, fooddelivery.Authorizing)
	assertConfig(t, "final", result.FinalConfig, fooddelivery.Delivered)

	// The whole point of time travel: an earlier reconstruction is not the final state.
	if configEqual(result.EarlierConfig, result.FinalConfig) {
		t.Fatalf("earlier config %v should differ from final config %v",
			result.EarlierConfig, result.FinalConfig)
	}

	// The log grows monotonically across the lifecycle: the last step folded at least
	// as many milestones as the first.
	if first, last := result.Timeline[0].LogLen, result.Timeline[len(result.Timeline)-1].LogLen; last < first {
		t.Fatalf("log length should not shrink across the lifecycle: first=%d last=%d", first, last)
	}
}

// assertConfig fails the test unless got equals want exactly (order-sensitive).
func assertConfig(t *testing.T, at string, got []fooddelivery.Stage, want ...fooddelivery.Stage) {
	t.Helper()
	if !configEqual(got, want) {
		t.Fatalf("%s: configuration = %v, want %v", at, got, want)
	}
}

// configEqual reports whether two configurations are identical (order-sensitive).
func configEqual(a, b []fooddelivery.Stage) bool {
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

// logHas reports whether log contains entry.
func logHas(log []string, entry string) bool {
	for _, e := range log {
		if e == entry {
			return true
		}
	}
	return false
}
