//go:build wasip1

// Command loopguest is a runaway WebAssembly guest used to exercise the host's
// timeout/cancellation: its eval spins in an unbounded loop and never returns, so
// a host that built the runtime with WithCloseOnContextDone(true) interrupts it
// when the call context is canceled or hits its deadline, returning an error
// rather than blocking forever.
package main

import "unsafe"

func main() {}

var inBuf [1 << 10]byte

func ptrOf(p *byte) uint32 { return uint32(uintptr(unsafe.Pointer(p))) }

//go:wasmexport alloc
func alloc(uint32) uint32 { return ptrOf(&inBuf[0]) }

// eval never returns: it spins on a volatile-ish accumulator the compiler cannot
// fold away, so only the host's context-done interruption can stop it.
//
//go:wasmexport eval
func eval(uint32, uint32) uint64 {
	var n uint64
	for {
		n++
		inBuf[n%uint64(len(inBuf))] = byte(n)
	}
}
