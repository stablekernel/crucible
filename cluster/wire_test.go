package cluster_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/cluster"
)

// TestWire_DeliverDecodesEvent confirms DeliverWire decodes a JSON event into the
// system's event type and delivers it — the receive half of a network transport.
func TestWire_DeliverDecodesEvent(t *testing.T) {
	ctx := context.Background()
	sys, ref := spawnedSystem(t, "node-a")

	// A sending node would marshal the event; here it is the string "finish".
	eventJSON, err := json.Marshal("finish")
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	delivered, err := sys.DeliverWire(ctx, ref, eventJSON)
	if err != nil {
		t.Fatalf("DeliverWire: %v", err)
	}
	if !delivered {
		t.Fatal("DeliverWire = false, want true")
	}
	if sys.Running() != 0 {
		t.Fatalf("Running() after wire finish = %d, want 0", sys.Running())
	}
}

// TestWire_SpawnDecodesInput confirms SpawnWire decodes a JSON input map and spawns
// a local actor stamped with the node.
func TestWire_SpawnDecodesInput(t *testing.T) {
	ctx := context.Background()
	sys := registeredSystem("node-b")

	inputJSON, err := json.Marshal(map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	ref, err := sys.SpawnWire(ctx, "child", "w-wire", inputJSON)
	if err != nil {
		t.Fatalf("SpawnWire: %v", err)
	}
	if ref.ID != "w-wire" || ref.Node != "node-b" {
		t.Fatalf("ref = %+v, want ID=w-wire Node=node-b", ref)
	}
	if sys.Running() != 1 {
		t.Fatalf("Running() = %d, want 1", sys.Running())
	}
}

// TestWire_EmptyPayloads confirms empty/nil wire payloads decode to the zero event
// and a nil input rather than erroring.
func TestWire_EmptyPayloads(t *testing.T) {
	ctx := context.Background()
	sys := registeredSystem("node-a")
	if _, err := sys.SpawnWire(ctx, "child", "w-empty", nil); err != nil {
		t.Fatalf("SpawnWire(nil input): %v", err)
	}
	ref, _ := sys.Ref("w-empty")
	// The empty event decodes to "" and is a no-op the actor ignores.
	if _, err := sys.DeliverWire(ctx, ref, nil); err != nil {
		t.Fatalf("DeliverWire(nil event): %v", err)
	}
}

// TestWire_BadPayloadErrors confirms malformed JSON surfaces a decode error rather
// than being silently dropped.
func TestWire_BadPayloadErrors(t *testing.T) {
	ctx := context.Background()
	sys, ref := spawnedSystem(t, "node-a")
	if _, err := sys.DeliverWire(ctx, ref, []byte("{not json")); err == nil {
		t.Fatal("DeliverWire(bad json) = nil error, want a decode error")
	}
	if _, err := sys.SpawnWire(ctx, "child", "x", []byte("{not json")); err == nil {
		t.Fatal("SpawnWire(bad json) = nil error, want a decode error")
	}
}

// TestWire_SystemSatisfiesWireEndpoint is a compile-time check that *System is a
// WireEndpoint, so a network transport can hold it type-erased.
func TestWire_SystemSatisfiesWireEndpoint(t *testing.T) {
	var _ cluster.WireEndpoint = cluster.NewSystem[string, string, *parentEnt]("n", nil)
}
