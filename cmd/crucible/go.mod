module github.com/stablekernel/crucible/cmd/crucible

go 1.25.11

require (
	github.com/stablekernel/crucible/gen v0.0.0
	github.com/stablekernel/crucible/state v0.0.0
)

replace github.com/stablekernel/crucible/state => ../../state

replace github.com/stablekernel/crucible/gen => ../../gen
