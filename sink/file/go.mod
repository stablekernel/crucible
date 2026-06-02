module github.com/stablekernel/crucible/sink/file

go 1.25.0

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry

require github.com/stablekernel/crucible/sink v0.0.0-00010101000000-000000000000

require github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
