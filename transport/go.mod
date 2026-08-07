module github.com/stablekernel/crucible/transport

go 1.25.11

toolchain go1.26.4

replace github.com/stablekernel/crucible/state => ../state

replace github.com/stablekernel/crucible/cluster => ../cluster

require (
	github.com/stablekernel/crucible/cluster v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/state v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.83.0
)

require (
	github.com/stablekernel/crucible/durable v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/stablekernel/crucible/durable => ../durable
