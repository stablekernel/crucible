//go:build wasip1

// Command approvalguest is a WebAssembly guest implementing a Crucible behavior
// guard over the JSON ABI: it reads a {"context": {"amount": N}} request and
// returns {"ok": bool}, admitting an order whose amount is at or above the
// approval threshold. The e2e wasm joint compiles it to wasip1/wasm on demand
// (GOOS=wasip1 GOARCH=wasm, -buildmode=c-shared) and runs it through wazero, so a
// guard whose truth lives in a foreign module gates a state-machine transition
// exactly like an in-tree guard. No binary is committed; the test builds it.
package main

import (
	"encoding/json"
	"unsafe"
)

func main() {}

// approvalThreshold is the amount at or above which an order is approved. The
// host test drives one order above it and one below it so both verdicts are
// exercised through the WASM evaluator.
const approvalThreshold = 100

// Fixed input and output buffers at stable linear-memory addresses, so the host
// writes the request to alloc's pointer and reads the response from eval's
// returned pointer without a real allocator — the convention the wasm package's
// own reference guest uses.
var (
	inBuf  [16 << 10]byte
	outBuf [16 << 10]byte
)

func ptrOf(p *byte) uint32 { return uint32(uintptr(unsafe.Pointer(p))) }

// alloc returns the address of the input buffer for the host to write size bytes
// into. The buffer is fixed; size must not exceed it.
//
//nolint:unparam // ABI: alloc must accept the requested size even though the buffer is fixed.
//go:wasmexport alloc
func alloc(size uint32) uint32 {
	_ = size
	return ptrOf(&inBuf[0])
}

// request is the guard envelope: the read-only context the guard evaluates.
type request struct {
	Context struct {
		Amount int64 `json:"amount"`
	} `json:"context"`
}

// response is the guard verdict envelope.
type response struct {
	OK bool `json:"ok"`
}

// eval reads the JSON request the host wrote at the input buffer, evaluates the
// approval predicate amount >= threshold, writes the JSON response into the
// output buffer, and returns a packed (outPtr<<32 | outLen). A malformed request
// is fail-safe: it returns ok=false rather than erroring.
//
//go:wasmexport eval
func eval(ptr, size uint32) uint64 {
	_ = ptr // input is always at inBuf; the host wrote it there via alloc.
	var req request
	if err := json.Unmarshal(inBuf[:size], &req); err != nil {
		return write(response{OK: false})
	}
	return write(response{OK: req.Context.Amount >= approvalThreshold})
}

// write marshals the response into the output buffer and returns the packed
// pointer and length the host unpacks.
func write(resp response) uint64 {
	b, _ := json.Marshal(resp)
	n := copy(outBuf[:], b)
	return uint64(ptrOf(&outBuf[0]))<<32 | uint64(n)
}
