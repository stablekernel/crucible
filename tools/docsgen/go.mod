module github.com/stablekernel/crucible/tools/docsgen

go 1.25.11

// The generator builds real example machines and renders their diagrams, and
// shells out to gomarkdoc for the API reference. It depends on the local state
// kernel, the rich-expression tier, and the flagship example, all via replace
// so the docs always reflect the working tree (never a published version).
replace github.com/stablekernel/crucible/state => ../../state

replace github.com/stablekernel/crucible/state/expr => ../../state/expr

replace github.com/stablekernel/crucible/examples/fooddelivery => ../../examples/fooddelivery

require (
	github.com/stablekernel/crucible/examples/fooddelivery v0.0.0-00010101000000-000000000000
	github.com/stablekernel/crucible/state v0.0.0-00010101000000-000000000000
)

require (
	cel.dev/expr v0.25.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/google/cel-go v0.28.1 // indirect
	github.com/stablekernel/crucible/state/expr v0.0.0-00010101000000-000000000000 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20260209203927-2842357ff358 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
