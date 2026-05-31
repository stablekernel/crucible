//go:build wasip1

// Command generousguest is a WebAssembly guest that implements the food-delivery
// "generous order" guard over the Crucible JSON ABI: it reads a {"context": {...order
// fields...}} request and returns {"ok": bool}, where the verdict is the same predicate
// the default CEL guard compiles — subtotal + tip >= 6000. It is compiled to
// wasip1/wasm by the dispatch package's polyglot test (GOOS=wasip1 GOARCH=wasm,
// -buildmode=c-shared) and run through wazero, so the guard's truth lives in the module
// rather than the kernel, yet gates the order machine's Authorized transition exactly
// like the CEL guard. No binary is committed; the test builds it on demand.
package main

import (
	"encoding/json"
	"unsafe"
)

func main() {}

// generousThreshold is the subtotal+tip floor (in cents) at or above which an order is
// generous. It mirrors the CEL predicate source the host guard reproduces.
const generousThreshold = 6000

// Fixed input and output buffers at stable linear-memory addresses (package globals),
// so the host writes the request to alloc's pointer and reads the response from eval's
// returned pointer without a real allocator — the same convention the wasm package's
// own guest uses.
var (
	inBuf  [16 << 10]byte
	outBuf [16 << 10]byte
)

func ptrOf(p *byte) uint32 { return uint32(uintptr(unsafe.Pointer(p))) }

// alloc returns the address of the input buffer for the host to write size bytes into.
// The buffer is fixed; size must not exceed it.
//
//nolint:unparam // ABI: alloc must accept the requested size even though the buffer is fixed.
//go:wasmexport alloc
func alloc(size uint32) uint32 {
	_ = size
	return ptrOf(&inBuf[0])
}

// request is the guard envelope: the read-only order context the guard evaluates. Only
// the fields the generous predicate needs are decoded; the rest of the order is ignored.
type request struct {
	Context struct {
		Subtotal int64 `json:"subtotal"`
		Tip      int64 `json:"tip"`
	} `json:"context"`
}

// response is the guard verdict envelope.
type response struct {
	OK bool `json:"ok"`
}

// eval reads the JSON request at the input buffer (the host wrote size bytes there via
// alloc), evaluates the generous predicate subtotal+tip >= 6000, writes the JSON
// response into the output buffer, and returns a packed (outPtr<<32 | outLen). A
// malformed request is fail-safe: it returns ok=false rather than erroring.
//
//go:wasmexport eval
func eval(ptr, size uint32) uint64 {
	_ = ptr // input is always at inBuf; the host wrote it there via alloc.
	var req request
	if err := json.Unmarshal(inBuf[:size], &req); err != nil {
		return write(response{OK: false})
	}
	ok := req.Context.Subtotal+req.Context.Tip >= generousThreshold
	return write(response{OK: ok})
}

// write marshals the response into the output buffer and returns the packed pointer and
// length the host unpacks.
func write(resp response) uint64 {
	b, _ := json.Marshal(resp)
	n := copy(outBuf[:], b)
	return uint64(ptrOf(&outBuf[0]))<<32 | uint64(n)
}
