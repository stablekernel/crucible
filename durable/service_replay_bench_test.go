package durable_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// benchSettles is how many service results the replay benchmark's journal holds.
const benchSettles = 200

// pingPongMachine alternates between a and b, each invoking fetch, so every settle
// enters the other state and starts its service: a journal of settled results as long
// as the benchmark wants.
func pingPongMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("pp").
		Service("fetch", fn).
		State("a").Invoke("fetch", state.WithInvokeOnDone("next")).
		State("b").Invoke("fetch", state.WithInvokeOnDone("next")).
		Initial("a").
		Transition("a").On("next").GoTo("b").
		Transition("b").On("next").GoTo("a").
		Quench()
}

func benchOpts(fn state.ServiceFn[svcCtx]) []durable.Option[string, string, svcCtx] {
	return []durable.Option[string, string, svcCtx]{
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		decoderFor("next", durable.DecodeAs[scnUser]),
	}
}

// runPingPong settles n results on h, alternating the two services.
func runPingPong(b *testing.B, h *durable.Handle[string, string, svcCtx], n int) {
	b.Helper()
	ids := [2]string{state.InvokeID("pp", "a", 0), state.InvokeID("pp", "b", 0)}
	for i := 0; i < n; i++ {
		if _, ok, err := h.RunService(context.Background(), ids[i%2]); !ok || err != nil {
			b.Fatalf("RunService %d = %v, %v", i, ok, err)
		}
	}
}

// BenchmarkRecover_ServiceResults measures Recover replaying benchSettles settled
// service results, each decoded through a decoder for onDone.
func BenchmarkRecover_ServiceResults(b *testing.B) {
	fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
	m := pingPongMachine(fn)
	store := durable.NewMemStore()
	opts := benchOpts(fn)
	h, err := durable.NewRunner(m, store, opts...).Start(context.Background(), "pp-1", svcCtx{}, state.WithInitialState("a"))
	if err != nil {
		b.Fatalf("Start: %v", err)
	}
	runPingPong(b, h, benchSettles)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := durable.Recover(context.Background(), m, store, "pp-1", opts...); err != nil {
			b.Fatalf("Recover: %v", err)
		}
	}
}

// BenchmarkRunService_Settle measures a live RunService that settles a result through
// a decoder for onDone.
func BenchmarkRunService_Settle(b *testing.B) {
	fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
	m := pingPongMachine(fn)
	h, err := durable.NewRunner(m, durable.NewMemStore(), benchOpts(fn)...).Start(context.Background(), "pp-1", svcCtx{}, state.WithInitialState("a"))
	if err != nil {
		b.Fatalf("Start: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	runPingPong(b, h, b.N)
}
