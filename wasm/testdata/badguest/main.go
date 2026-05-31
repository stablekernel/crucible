//go:build wasip1

// Command badguest is a misbehaving WebAssembly guest used to exercise the host's
// defensive ABI bounds-checking: its eval returns a packed pointer/length that
// points far outside linear memory, so the host's response Read must fail cleanly
// rather than read out of bounds.
package main

import "unsafe"

func main() {}

var inBuf [1 << 10]byte

func ptrOf(p *byte) uint32 { return uint32(uintptr(unsafe.Pointer(p))) }

//go:wasmexport alloc
func alloc(uint32) uint32 { return ptrOf(&inBuf[0]) }

// eval ignores the request and returns an out-of-range (pointer, length) so the
// host's Read of the response is rejected.
//
//go:wasmexport eval
func eval(uint32, uint32) uint64 {
	const wayOutOfRange = 1 << 31
	return uint64(wayOutOfRange)<<32 | uint64(wayOutOfRange)
}
