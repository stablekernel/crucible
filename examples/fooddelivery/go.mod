module github.com/stablekernel/crucible/examples/fooddelivery

go 1.25.0

replace github.com/stablekernel/crucible/state => ../../state

replace github.com/stablekernel/crucible/state/expr => ../../state/expr

require (
	github.com/stablekernel/crucible/state v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/state/expr v0.0.0-00010101000000-000000000000
)

require (
	cel.dev/expr v0.25.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/google/cel-go v0.28.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20260209203927-2842357ff358 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251222181119-0a764e51fe1b // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
