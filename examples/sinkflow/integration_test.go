// SPDX-License-Identifier: Apache-2.0

//go:build integration

package sinkflow_test

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	gohttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/bridge"
	filesink "github.com/stablekernel/crucible/sink/file"
	httpsink "github.com/stablekernel/crucible/sink/http"
	sqlsink "github.com/stablekernel/crucible/sink/sql"
	"github.com/stablekernel/crucible/state"
)

// orderIT is the entity the integration machine advances.
type orderIT struct {
	ID    string
	Stage string
}

const (
	placedIT    = "placed"
	preparingIT = "preparing"
	enRouteIT   = "enroute"
	deliveredIT = "delivered"

	prepareIT  = "prepare"
	dispatchIT = "dispatch"
	deliverIT  = "deliver"
)

// TestIntegrationFlowFansToRealDestinations wires a running order machine
// through a sink.Manifold into three real hermetic destinations (a SQLite DB, a
// live httptest server, and a temp-dir JSONL file) plus a deliberately failing
// outlet. It asserts every destination actually received its transitions, the
// emit span nests under the transition span, and the induced failure surfaces on
// both the slog logger and the sink.failed counter.
func TestIntegrationFlowFansToRealDestinations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Real SQLite destination.
	db, err := stdsql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.ExecContext(ctx, `CREATE TABLE transitions (event TEXT NOT NULL, to_state TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	sqlReg := sqlsink.NewRegistry()
	csink.Register(sqlReg, func(_ context.Context, tr bridge.Transition) csink.Op[sqlsink.Tx] {
		return sqlsink.Exec("INSERT INTO transitions(event, to_state) VALUES (?, ?)", tr.Event, tr.To)
	})
	sqlOutlet := sqlsink.New(db, sqlReg)

	// Real HTTP destination capturing each posted transition.
	var (
		mu        sync.Mutex
		posted    []bridge.Transition
		postCount int
	)
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		body, _ := io.ReadAll(r.Body)
		var tr bridge.Transition
		_ = json.Unmarshal(body, &tr)
		mu.Lock()
		posted = append(posted, tr)
		postCount++
		mu.Unlock()
		w.WriteHeader(gohttp.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	httpReg := httpsink.NewRegistry()
	csink.Register(httpReg, func(_ context.Context, tr bridge.Transition) csink.Op[httpsink.Doer] {
		return httpsink.PostJSON(srv.URL+"/transitions", tr)
	})
	httpOutlet := httpsink.New(srv.Client(), httpReg)

	// Real file destination in a temp dir.
	filePath := filepath.Join(t.TempDir(), "transitions.jsonl")
	fileOutlet, err := filesink.Open(filePath)
	if err != nil {
		t.Fatalf("file.Open() error = %v", err)
	}

	// Deliberately failing outlet to drive the failure-visibility assertions.
	wantErr := errors.New("warehouse rejected dispatch")
	failing := csink.OutletFunc(func(_ context.Context, payload any) error {
		tr, ok := payload.(bridge.Transition)
		if !ok {
			return csink.ErrUnregistered
		}
		if tr.Event == dispatchIT {
			return &csink.Error{Outlet: "warehouse", Phase: csink.PhaseApply, PayloadType: "bridge.Transition", Err: wantErr}
		}
		return nil
	})

	tracer := &recTracer{}
	meter := newFakeMeter()
	handler := &captureHandler{}

	manifold := csink.NewManifold(
		csink.WithLogger(slog.New(handler)),
		csink.WithTracer(tracer),
		csink.WithMeter(meter),
		csink.WithOutlets(sqlOutlet, httpOutlet, fileOutlet, failing),
	)

	machine := state.Forge[string, string, *orderIT]("order").
		Use(bridge.Middleware[string, string, *orderIT](manifold, bridge.WithTracer(tracer))).
		State(placedIT).State(preparingIT).State(enRouteIT).State(deliveredIT).
		Initial(placedIT).
		CurrentStateFn(func(o *orderIT) string { return o.Stage }).
		Transition(placedIT).On(prepareIT).GoTo(preparingIT).
		Transition(preparingIT).On(dispatchIT).GoTo(enRouteIT).
		Transition(enRouteIT).On(deliverIT).GoTo(deliveredIT).
		Quench(state.Strict())

	inst := machine.Cast(&orderIT{ID: "order-1", Stage: placedIT})
	for _, ev := range []string{prepareIT, dispatchIT, deliverIT} {
		inst.Fire(ctx, ev)
	}
	if final := inst.Current(); final != deliveredIT {
		t.Fatalf("final stage = %q, want %q", final, deliveredIT)
	}

	if err = manifold.Shutdown(ctx); err != nil {
		t.Fatalf("manifold.Shutdown() error = %v", err)
	}

	assertSQLLanded(t, ctx, db)
	assertHTTPLanded(t, &mu, posted)
	assertFileLanded(t, filePath)
	assertSpanNesting(t, tracer)
	assertFailureVisible(t, meter, handler)
}

func assertSQLLanded(t *testing.T, ctx context.Context, db *stdsql.DB) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT event, to_state FROM transitions ORDER BY rowid`)
	if err != nil {
		t.Fatalf("query transitions error = %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var ev, to string
		if err = rows.Scan(&ev, &to); err != nil {
			t.Fatalf("scan error = %v", err)
		}
		got = append(got, ev+"->"+to)
	}
	if err = rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
	want := []string{"prepare->preparing", "dispatch->enroute", "deliver->delivered"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sql rows = %v, want %v", got, want)
	}
}

func assertHTTPLanded(t *testing.T, mu *sync.Mutex, posted []bridge.Transition) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if len(posted) != 3 {
		t.Fatalf("http server received %d transitions, want 3", len(posted))
	}
	if posted[1].Event != "dispatch" || posted[1].To != "enroute" {
		t.Errorf("http transition[1] = %+v, want dispatch->enroute", posted[1])
	}
}

func assertFileLanded(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("file has %d lines, want 3 (%q)", len(lines), data)
	}
	var first bridge.Transition
	if err = json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode file line error = %v", err)
	}
	if first.Event != "prepare" || first.To != "preparing" {
		t.Errorf("file line[0] = %+v, want prepare->preparing", first)
	}
}

func assertSpanNesting(t *testing.T, tr *recTracer) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	names := map[string]string{}
	for _, s := range tr.spans {
		names[s.id] = s.name
	}
	var transitions, nestedEmits int
	for _, s := range tr.spans {
		switch s.name {
		case "state.transition":
			transitions++
		case "sink.Sink":
			if names[s.parent] == "state.transition" {
				nestedEmits++
			}
		}
	}
	if transitions != 3 {
		t.Errorf("state.transition spans = %d, want 3", transitions)
	}
	if nestedEmits != 3 {
		t.Errorf("sink.Sink spans nested under a transition = %d, want 3", nestedEmits)
	}
}

func assertFailureVisible(t *testing.T, meter *fakeMeter, handler *captureHandler) {
	t.Helper()
	if got := meter.value("sink.failed"); got != 1 {
		t.Errorf("sink.failed = %d, want 1", got)
	}
	if got := handler.errorCount(); got != 1 {
		t.Errorf("error log records = %d, want 1", got)
	}
}
