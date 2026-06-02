// SPDX-License-Identifier: Apache-2.0

package timestream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/sinktest"
	tsink "github.com/stablekernel/crucible/sink/timestream"
)

// fakeClient is a hand-rolled Client implementation — no mockery.
type fakeClient struct {
	calls []*timestreamwrite.WriteRecordsInput
	err   error
}

func (f *fakeClient) WriteRecords(
	_ context.Context,
	params *timestreamwrite.WriteRecordsInput,
	_ ...func(*timestreamwrite.Options),
) (*timestreamwrite.WriteRecordsOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &timestreamwrite.WriteRecordsOutput{}, nil
}

// metricRecorded is a sample payload for tests.
type metricRecorded struct {
	Name  string
	Value string
}

func newOutlet(c tsink.Client) csink.Outlet {
	reg := tsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, m metricRecorded) csink.Op[tsink.Client] {
		db := "metrics"
		table := "readings"
		return tsink.WriteRecords(&timestreamwrite.WriteRecordsInput{
			DatabaseName: &db,
			TableName:    &table,
			Records: []types.Record{
				{MeasureName: &m.Name, MeasureValue: &m.Value},
			},
		})
	})
	return tsink.New(c, reg)
}

func TestWriteRecords_SendsInput(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), metricRecorded{Name: "cpu", Value: "0.42"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.calls) != 1 {
		t.Fatalf("WriteRecords calls = %d, want 1", len(c.calls))
	}
	got := c.calls[0]
	if got.DatabaseName == nil || *got.DatabaseName != "metrics" {
		t.Fatalf("DatabaseName = %v, want metrics", got.DatabaseName)
	}
	if got.TableName == nil || *got.TableName != "readings" {
		t.Fatalf("TableName = %v, want readings", got.TableName)
	}
	if len(got.Records) != 1 {
		t.Fatalf("Records len = %d, want 1", len(got.Records))
	}
}

func TestWriteRecords_UnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := newOutlet(&fakeClient{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestWriteRecords_ErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("throttling exception")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), metricRecorded{Name: "cpu", Value: "0.42"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "timestream" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:timestream, Phase:apply}", se)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
