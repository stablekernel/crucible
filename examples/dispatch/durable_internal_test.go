package dispatch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
)

// TestStartActiveOrder_DrivesToActive exercises the shared live-run prelude directly:
// it starts an order in a memory store and asserts the helper drives it into the
// Active fulfillment configuration (Cooking + OnTime), returning a usable model and
// live Handle.
func TestStartActiveOrder_DrivesToActive(t *testing.T) {
	ctx := context.Background()
	store := durable.NewMemStore()
	opts := durableOptions(state.NewFakeClock(fixedClockStart))

	model, h, err := startActiveOrder(ctx, store, durable.InstanceID("order-x"), opts)
	if err != nil {
		t.Fatalf("startActiveOrder: %v", err)
	}
	if model == nil || h == nil {
		t.Fatal("startActiveOrder returned a nil model or handle")
	}
	got := h.Instance().Configuration()
	want := []fooddelivery.Stage{fooddelivery.Cooking, fooddelivery.OnTime}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("configuration = %v, want %v", got, want)
	}
}

// TestStartActiveOrder_ExistingInstance covers the start-error branch: starting an
// instance id already present in the store surfaces an error rather than clobbering
// the recorded baseline.
func TestStartActiveOrder_ExistingInstance(t *testing.T) {
	ctx := context.Background()
	store := durable.NewMemStore()
	opts := durableOptions(state.NewFakeClock(fixedClockStart))
	id := durable.InstanceID("order-dup")

	if _, _, err := startActiveOrder(ctx, store, id, opts); err != nil {
		t.Fatalf("first startActiveOrder: %v", err)
	}
	_, _, err := startActiveOrder(ctx, store, id, opts)
	if err == nil {
		t.Fatal("expected an error starting an already-recorded instance, got nil")
	}
	if !errors.Is(err, durable.ErrInstanceExists) {
		t.Fatalf("error = %v, want ErrInstanceExists", err)
	}
}

// failingStore wraps a durable.Store and fails Load / History after a configured
// number of successful Loads, so the harness's recovery and reconstruction error
// branches are exercised without disturbing the live recording run (whose only Load
// is Start's existence probe). Writes always pass through.
type failingStore struct {
	durable.Store
	allowLoads   int // successful Loads before failing; History counts against it too
	allowAppends int // successful Appends before failing; -1 (default) never fails Append
}

func (s *failingStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	if s.allowAppends == 0 {
		return 0, errors.New("injected append failure")
	}
	if s.allowAppends > 0 {
		s.allowAppends--
	}
	return s.Store.Append(ctx, id, rec, opts...)
}

func (s *failingStore) Load(ctx context.Context, id durable.InstanceID) ([]byte, []durable.Record, error) {
	if s.allowLoads <= 0 {
		return nil, nil, errors.New("injected load failure")
	}
	s.allowLoads--
	return s.Store.Load(ctx, id)
}

func (s *failingStore) History(ctx context.Context, id durable.InstanceID) ([]byte, []durable.Record, error) {
	if s.allowLoads <= 0 {
		return nil, nil, errors.New("injected history failure")
	}
	s.allowLoads--
	hs, ok := s.Store.(durable.HistoryStore)
	if !ok {
		return nil, nil, durable.ErrInstanceNotFound
	}
	return hs.History(ctx, id)
}

// TestCrashRecovery_RecoverError covers the crash-recovery core's recover-error
// branch: the live run records the order (consuming Start's one probing Load), then
// the post-crash Recover's Load fails, so crashRecovery returns a wrapped error.
func TestCrashRecovery_RecoverError(t *testing.T) {
	ctx := context.Background()
	// One Load allowed: Start's existence probe during the live run. The post-crash
	// Recover's Load then fails.
	store := &failingStore{Store: durable.NewMemStore(), allowLoads: 1, allowAppends: -1}

	_, err := crashRecovery(ctx, store, durable.InstanceID("order-recover-fail"))
	if err == nil {
		t.Fatal("expected a recover error from the failing store, got nil")
	}
	if !strings.Contains(err.Error(), "recover order") {
		t.Fatalf("error = %v, want a recover-order message", err)
	}
}

// TestTimeTravel_StepsError covers the time-travel core's enumeration-error branch:
// the live run records the order (consuming Start's one probing Load), then the Steps
// History read fails, so timeTravel returns a wrapped error.
func TestTimeTravel_StepsError(t *testing.T) {
	ctx := context.Background()
	store := &failingStore{Store: durable.NewMemStore(durable.WithHistory()), allowLoads: 1, allowAppends: -1}

	_, err := timeTravel(ctx, store, durable.InstanceID("order-steps-fail"))
	if err == nil {
		t.Fatal("expected a steps error from the failing store, got nil")
	}
	if !strings.Contains(err.Error(), "enumerate steps") {
		t.Fatalf("error = %v, want an enumerate-steps message", err)
	}
}

// TestCrashRecovery_DriveError covers the crash-recovery core's drive-error branch:
// the live run and Recover succeed, then an Append failure during the post-recovery
// drive makes driveToDelivered fail, so crashRecovery surfaces the error. It allows
// the two live-run Appends (Submit + the authorize settle) and the two recovery
// Loads (Start probe + Recover), then fails the first post-recovery Append.
func TestCrashRecovery_DriveError(t *testing.T) {
	ctx := context.Background()
	store := &failingStore{Store: durable.NewMemStore(), allowLoads: 2, allowAppends: 2}

	_, err := crashRecovery(ctx, store, durable.InstanceID("order-drive-fail"))
	if err == nil {
		t.Fatal("expected a drive error from the failing store, got nil")
	}
	if !strings.Contains(err.Error(), "kitchen actor") {
		t.Fatalf("error = %v, want a kitchen-actor (deliver) message", err)
	}
}

// TestTimeTravel_StateAtError covers the reconstruction-error branch inside the
// timeline loop: the live run and Steps succeed, then the first StateAt's History
// read fails, so timeTravel surfaces a wrapped reconstruct error.
func TestTimeTravel_StateAtError(t *testing.T) {
	ctx := context.Background()
	// Allow Start's probe Load and the Steps History read; fail the first StateAt.
	store := &failingStore{Store: durable.NewMemStore(durable.WithHistory()), allowLoads: 2, allowAppends: -1}

	_, err := timeTravel(ctx, store, durable.InstanceID("order-stateat-fail"))
	if err == nil {
		t.Fatal("expected a reconstruct error from the failing store, got nil")
	}
	if !strings.Contains(err.Error(), "reconstruct step") {
		t.Fatalf("error = %v, want a reconstruct-step message", err)
	}
}

// TestTimeTravel_EarlierStateAtError covers the earlier-reconstruction error branch:
// the live run, Steps, and every timeline StateAt succeed, then the earlier-step
// StateAt's History read fails, so timeTravel surfaces a wrapped error. It allows the
// Start probe (1) + Steps (1) + the five timeline reconstructions (5) and fails the
// next History read (the earlier reconstruction).
func TestTimeTravel_EarlierStateAtError(t *testing.T) {
	ctx := context.Background()
	store := &failingStore{Store: durable.NewMemStore(durable.WithHistory()), allowLoads: 7, allowAppends: -1}

	_, err := timeTravel(ctx, store, durable.InstanceID("order-earlier-fail"))
	if err == nil {
		t.Fatal("expected an earlier-reconstruction error from the failing store, got nil")
	}
	if !strings.Contains(err.Error(), "earlier step") {
		t.Fatalf("error = %v, want an earlier-step message", err)
	}
}

// TestTimeTravel_FinalStateAtError covers the final-reconstruction error branch: the
// live run, Steps, every timeline StateAt, and the earlier StateAt succeed, then the
// final-step StateAt's History read fails. It allows the Start probe (1) + Steps (1)
// + five timeline reconstructions (5) + the earlier reconstruction (1) and fails the
// next read (the final reconstruction).
func TestTimeTravel_FinalStateAtError(t *testing.T) {
	ctx := context.Background()
	store := &failingStore{Store: durable.NewMemStore(durable.WithHistory()), allowLoads: 8, allowAppends: -1}

	_, err := timeTravel(ctx, store, durable.InstanceID("order-final-fail"))
	if err == nil {
		t.Fatal("expected a final-reconstruction error from the failing store, got nil")
	}
	if !strings.Contains(err.Error(), "final step") {
		t.Fatalf("error = %v, want a final-step message", err)
	}
}

// TestCompleteActor_StaleRef covers the not-delivered branch: delivering to an actor
// ref that names no running actor (it was already settled) reports the actor was not
// running rather than silently succeeding.
func TestCompleteActor_StaleRef(t *testing.T) {
	ctx := context.Background()
	store := durable.NewMemStore()
	opts := durableOptions(state.NewFakeClock(fixedClockStart))

	_, h, err := startActiveOrder(ctx, store, durable.InstanceID("order-stale"), opts)
	if err != nil {
		t.Fatalf("startActiveOrder: %v", err)
	}
	// Complete the kitchen actor once so its ref is now stale (the actor has settled).
	if err = completeActor(ctx, h, "kitchen", fooddelivery.Cooking, fooddelivery.KitchenCook); err != nil {
		t.Fatalf("first completeActor: %v", err)
	}
	// The kitchen actor id no longer names a running actor, so ActorRef reports false.
	err = completeActor(ctx, h, "kitchen", fooddelivery.Cooking, fooddelivery.KitchenCook)
	if err == nil {
		t.Fatal("expected an error completing an already-settled actor, got nil")
	}
	if !strings.Contains(err.Error(), "kitchen") {
		t.Fatalf("error = %v, want a kitchen message", err)
	}
}

// TestDriveToDelivered_NoKitchenActor covers the no-actor error branch: driving a
// handle that has not reached the Cooking state (so no kitchen actor is running)
// reports a clear error rather than panicking or silently succeeding.
func TestDriveToDelivered_NoKitchenActor(t *testing.T) {
	ctx := context.Background()
	store := durable.NewMemStore()
	opts := durableOptions(state.NewFakeClock(fixedClockStart))

	model, err := fooddelivery.NewModel()
	if err != nil {
		t.Fatalf("NewModel: %v", err)
	}
	runner := durable.NewRunner(model, store, opts...)
	// Start but do NOT authorize: the order rests in Placed with no kitchen actor.
	h, err := runner.Start(ctx, durable.InstanceID("order-y"), sampleOrder(),
		state.WithInitialState(fooddelivery.Placed))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	err = driveToDelivered(ctx, h)
	if err == nil {
		t.Fatal("expected an error driving an order with no kitchen actor, got nil")
	}
	if !strings.Contains(err.Error(), "kitchen actor") {
		t.Fatalf("error = %v, want a kitchen-actor message", err)
	}
}
