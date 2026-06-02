// SPDX-License-Identifier: Apache-2.0

package timestream_test

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	csink "github.com/stablekernel/crucible/sink"
	tsink "github.com/stablekernel/crucible/sink/timestream"
)

// recordingClient records WriteRecords calls without touching AWS.
type recordingClient struct{ tables []string }

func (r *recordingClient) WriteRecords(
	_ context.Context,
	params *timestreamwrite.WriteRecordsInput,
	_ ...func(*timestreamwrite.Options),
) (*timestreamwrite.WriteRecordsOutput, error) {
	if params.TableName != nil {
		r.tables = append(r.tables, *params.TableName)
	}
	return &timestreamwrite.WriteRecordsOutput{}, nil
}

type temperatureRead struct {
	Sensor  string
	Celsius string
}

func ExampleNew() {
	c := &recordingClient{}
	reg := tsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, t temperatureRead) csink.Op[tsink.Client] {
		db := "sensors"
		table := "temperature"
		return tsink.WriteRecords(&timestreamwrite.WriteRecordsInput{
			DatabaseName: &db,
			TableName:    &table,
			Records: []types.Record{
				{MeasureName: &t.Sensor, MeasureValue: &t.Celsius},
			},
		})
	})

	outlet := tsink.New(c, reg)
	_ = outlet.Sink(context.Background(), temperatureRead{Sensor: "sensor-1", Celsius: "21.5"})

	fmt.Println(c.tables[0])
	// Output: temperature
}
