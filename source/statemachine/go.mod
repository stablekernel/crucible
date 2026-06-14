module github.com/stablekernel/crucible/source/statemachine

go 1.25.11
toolchain go1.26.4
require (
	github.com/stablekernel/crucible/source v0.0.0
	github.com/stablekernel/crucible/state v0.0.0
	github.com/stablekernel/crucible/telemetry v0.0.0
)

replace github.com/stablekernel/crucible/source => ../

replace github.com/stablekernel/crucible/state => ../../state

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
