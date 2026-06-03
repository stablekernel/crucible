module github.com/stablekernel/crucible/examples/dispatch

go 1.25.11

replace github.com/stablekernel/crucible/state => ../../state

replace github.com/stablekernel/crucible/state/expr => ../../state/expr

replace github.com/stablekernel/crucible/examples/fooddelivery => ../fooddelivery

replace github.com/stablekernel/crucible/durable => ../../durable

replace github.com/stablekernel/crucible/cluster => ../../cluster

replace github.com/stablekernel/crucible/transport => ../../transport

replace github.com/stablekernel/crucible/wasm => ../../wasm

replace github.com/stablekernel/crucible/telemetry => ../../telemetry

replace github.com/stablekernel/crucible/telemetry/slog => ../../telemetry/slog

require (
	github.com/stablekernel/crucible/cluster v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/durable v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/examples/fooddelivery v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/state v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/telemetry v0.0.0
	github.com/stablekernel/crucible/telemetry/slog v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/transport v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/wasm v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.81.1
)

require (
	cel.dev/expr v0.25.2 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/google/cel-go v0.28.1 // indirect
	github.com/stablekernel/crucible/state/expr v0.0.0-00010101000000-000000000000 // indirect
	github.com/tetratelabs/wazero v1.12.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20260209203927-2842357ff358 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
