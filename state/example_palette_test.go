package state_test

import (
	"encoding/json"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// cart is the entity the palette example registers behavior against.
type cart struct {
	amount int
}

// ExampleRegistry_Palette registers a described guard and action, then prints the
// discoverable palette a visual builder reads to render a form for each ref. The
// palette is sorted deterministically (by kind, then name) and JSON-serializes
// cleanly for transport over a builder API.
func ExampleRegistry_Palette() {
	reg := state.NewRegistry[cart]()

	reg.Guard("minAmount", func(c state.GuardCtx[cart]) bool { return c.Entity.amount >= 1 },
		state.Describe("Passes when the amount is at least min.").
			Param("min", state.IntParam).
			OptionalParam("currency", state.StringParam).
			Reads("Cart"))

	reg.Action("charge", func(state.ActionCtx[cart]) (state.Effect, error) { return nil, nil },
		state.Describe("Charges the cart through the named gateway.").
			Param("gateway", state.StringParam).
			Writes("Cart"))

	out, _ := json.MarshalIndent(reg.Palette(), "", "  ")
	fmt.Println(string(out))

	// Output:
	// [
	//   {
	//     "kind": "action",
	//     "name": "charge",
	//     "description": "Charges the cart through the named gateway.",
	//     "params": [
	//       {
	//         "name": "gateway",
	//         "type": "string",
	//         "required": true
	//       }
	//     ],
	//     "writes": [
	//       "Cart"
	//     ]
	//   },
	//   {
	//     "kind": "guard",
	//     "name": "minAmount",
	//     "description": "Passes when the amount is at least min.",
	//     "params": [
	//       {
	//         "name": "min",
	//         "type": "int",
	//         "required": true
	//       },
	//       {
	//         "name": "currency",
	//         "type": "string"
	//       }
	//     ],
	//     "reads": [
	//       "Cart"
	//     ]
	//   }
	// ]
}
