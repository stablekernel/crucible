module github.com/stablekernel/crucible/sink/sql

go 1.25.11
toolchain go1.26.4
require (
	github.com/stablekernel/crucible/sink v0.0.0
	modernc.org/sqlite v1.51.0
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	modernc.org/libc v1.72.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
