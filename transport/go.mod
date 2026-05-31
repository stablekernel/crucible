module github.com/stablekernel/crucible/transport

go 1.25.0

replace github.com/stablekernel/crucible/state => ../state

replace github.com/stablekernel/crucible/cluster => ../cluster

require (
	github.com/stablekernel/crucible/cluster v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/state v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.81.1
)

require (
	github.com/stablekernel/crucible/durable v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/stablekernel/crucible/durable => ../durable
