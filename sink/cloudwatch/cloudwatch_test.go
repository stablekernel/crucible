// SPDX-License-Identifier: Apache-2.0

package cloudwatch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	csink "github.com/stablekernel/crucible/sink"
	cw "github.com/stablekernel/crucible/sink/cloudwatch"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled Client implementation. It records every
// PutLogEvents call and returns the configured error (nil by default).
type fakeClient struct {
	calls []*cloudwatchlogs.PutLogEventsInput
	err   error
}

func (f *fakeClient) PutLogEvents(_ context.Context, params *cloudwatchlogs.PutLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &cloudwatchlogs.PutLogEventsOutput{}, nil
}

// payload types used across tests.
type (
	appStarted struct{ AppName string }
	appStopped struct{ AppName string }
)

func ptr[T any](v T) *T { return &v }

// newOutlet wires a registry with both payload types and returns the outlet.
func newOutlet(c cw.Client) csink.Outlet {
	reg := cw.NewRegistry()
	csink.Register(reg, func(_ context.Context, e appStarted) csink.Op[cw.Client] {
		return cw.PutLogEvent("/app/events", "app-stream", "started:"+e.AppName)
	})
	csink.Register(reg, func(_ context.Context, e appStopped) csink.Op[cw.Client] {
		return cw.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
			LogGroupName:  ptr("/app/events"),
			LogStreamName: ptr("app-stream"),
			LogEvents: []types.InputLogEvent{
				{Message: ptr("stopped:" + e.AppName), Timestamp: ptr(int64(1000))},
			},
		})
	})
	return cw.New(c, reg)
}

func TestPutLogEvent_SendsEventToStream(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), appStarted{AppName: "api"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.calls) != 1 {
		t.Fatalf("PutLogEvents call count = %d, want 1", len(c.calls))
	}
	got := c.calls[0]
	if got.LogGroupName == nil || *got.LogGroupName != "/app/events" {
		t.Errorf("LogGroupName = %v, want /app/events", got.LogGroupName)
	}
	if got.LogStreamName == nil || *got.LogStreamName != "app-stream" {
		t.Errorf("LogStreamName = %v, want app-stream", got.LogStreamName)
	}
	if len(got.LogEvents) != 1 {
		t.Fatalf("LogEvents count = %d, want 1", len(got.LogEvents))
	}
	if got.LogEvents[0].Message == nil || *got.LogEvents[0].Message != "started:api" {
		t.Errorf("Message = %v, want started:api", got.LogEvents[0].Message)
	}
	if got.LogEvents[0].Timestamp == nil || *got.LogEvents[0].Timestamp <= 0 {
		t.Errorf("Timestamp = %v, want positive millis", got.LogEvents[0].Timestamp)
	}
}

func TestPutLogEvents_PassesThroughInput(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), appStopped{AppName: "worker"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.calls) != 1 {
		t.Fatalf("PutLogEvents call count = %d, want 1", len(c.calls))
	}
	got := c.calls[0]
	if got.LogGroupName == nil || *got.LogGroupName != "/app/events" {
		t.Errorf("LogGroupName = %v, want /app/events", got.LogGroupName)
	}
	if len(got.LogEvents) != 1 {
		t.Fatalf("LogEvents count = %d, want 1", len(got.LogEvents))
	}
	if got.LogEvents[0].Message == nil || *got.LogEvents[0].Message != "stopped:worker" {
		t.Errorf("Message = %v, want stopped:worker", got.LogEvents[0].Message)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type unknown struct{}
	err := newOutlet(&fakeClient{}).Sink(context.Background(), unknown{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestPutLogEvent_ErrorWrappedAsSinkError(t *testing.T) {
	t.Parallel()

	boom := errors.New("throttled")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), appStarted{AppName: "api"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want to wrap %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() = %v, want *sink.Error", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %v, want PhaseApply", se.Phase)
	}
	if se.Outlet != "cloudwatch" {
		t.Errorf("Outlet = %q, want cloudwatch", se.Outlet)
	}
}

func TestPutLogEvents_ErrorWrappedAsSinkError(t *testing.T) {
	t.Parallel()

	boom := errors.New("stream not found")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), appStopped{AppName: "worker"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want to wrap %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() = %v, want *sink.Error", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %v, want PhaseApply", se.Phase)
	}
	if se.Outlet != "cloudwatch" {
		t.Errorf("Outlet = %q, want cloudwatch", se.Outlet)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
