module github.com/stablekernel/crucible/e2e

go 1.25.11

toolchain go1.26.4

replace (
	github.com/stablekernel/crucible/cluster => ../cluster
	github.com/stablekernel/crucible/durable => ../durable
	github.com/stablekernel/crucible/sink => ../sink
	github.com/stablekernel/crucible/sink/bridge => ../sink/bridge
	github.com/stablekernel/crucible/source => ../source
	github.com/stablekernel/crucible/source/statemachine => ../source/statemachine
	github.com/stablekernel/crucible/state => ../state
	github.com/stablekernel/crucible/state/expr => ../state/expr
	github.com/stablekernel/crucible/telemetry => ../telemetry
	github.com/stablekernel/crucible/transport => ../transport
	github.com/stablekernel/crucible/wasm => ../wasm
)

require (
	github.com/stablekernel/crucible/cluster v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/durable v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/sink v0.0.0
	github.com/stablekernel/crucible/sink/bridge v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/source v0.0.0
	github.com/stablekernel/crucible/source/statemachine v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/state v1.0.0
	github.com/stablekernel/crucible/state/expr v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/telemetry v0.0.0
	github.com/stablekernel/crucible/transport v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/wasm v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.81.1
)

require (
	cel.dev/expr v0.25.2 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/google/cel-go v0.28.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/tetratelabs/wazero v1.12.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20260209203927-2842357ff358 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
