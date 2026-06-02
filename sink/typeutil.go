// SPDX-License-Identifier: Apache-2.0

package sink

import "reflect"

// typeName returns a stable, human-readable name for a payload's concrete type,
// used in error messages, log fields, and span attributes. The nil payload is
// reported as "<nil>" rather than panicking.
func typeName(v any) string {
	if v == nil {
		return "<nil>"
	}
	return reflect.TypeOf(v).String()
}
