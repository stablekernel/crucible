module github.com/stablekernel/crucible/sink/redis

go 1.25.0

require (
	github.com/redis/go-redis/v9 v9.20.0
	github.com/stablekernel/crucible/sink v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
