module github.com/stablekernel/crucible/sink/http

go 1.25.11

toolchain go1.26.4

require github.com/stablekernel/crucible/sink v0.0.0

require github.com/stablekernel/crucible/telemetry v0.0.0 // indirect

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
