module github.com/stablekernel/crucible/sink/kafka

go 1.25.0

require github.com/stablekernel/crucible/sink v0.0.0

require github.com/twmb/franz-go v1.21.2

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
