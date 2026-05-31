//go:build wasip1

// Command guardguest is a WebAssembly guest that implements a Crucible guard over
// the JSON ABI: it reads a {"context": {...}} request and returns {"ok": bool}. It
// is compiled to wasip1/wasm by the wasm package's tests (GOOS=wasip1 GOARCH=wasm)
// and run through wazero, proving the host ABI against a real guest. The predicate
// is fixed for the test: the order is approved when its status is "paid" and its
// total is at least the threshold.
package main

import (
	"encoding/json"
	"unsafe"
)

func main() {}

// Fixed input and output buffers at stable linear-memory addresses (package
// globals), so the host can write the request to alloc's pointer and read the
// response from eval's returned pointer without a real allocator.
var (
	inBuf  [16 << 10]byte
	outBuf [16 << 10]byte
)

func ptrOf(p *byte) uint32 { return uint32(uintptr(unsafe.Pointer(p))) }

// alloc returns the address of the input buffer for the host to write size bytes
// into. The buffer is fixed; size must not exceed it.
//
//go:wasmexport alloc
func alloc(size uint32) uint32 {
	_ = size
	return ptrOf(&inBuf[0])
}

type request struct {
	Context struct {
		Status string  `json:"status"`
		Total  float64 `json:"total"`
	} `json:"context"`
}

type response struct {
	OK bool `json:"ok"`
}

// eval reads the JSON request at [ptr, ptr+size), evaluates the guard, writes the
// JSON response into the output buffer, and returns a packed (outPtr<<32 | outLen).
//
//go:wasmexport eval
func eval(ptr, size uint32) uint64 {
	_ = ptr // input is always at inBuf; the host wrote it there via alloc
	var req request
	if err := json.Unmarshal(inBuf[:size], &req); err != nil {
		return write(response{OK: false})
	}
	ok := req.Context.Status == "paid" && req.Context.Total >= 40
	return write(response{OK: ok})
}

func write(resp response) uint64 {
	b, _ := json.Marshal(resp)
	n := copy(outBuf[:], b)
	return uint64(ptrOf(&outBuf[0]))<<32 | uint64(n)
}
