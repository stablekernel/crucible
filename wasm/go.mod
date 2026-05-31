module github.com/stablekernel/crucible/wasm

go 1.25.0

replace github.com/stablekernel/crucible/state => ../state

require github.com/tetratelabs/wazero v1.12.0

require golang.org/x/sys v0.44.0 // indirect
