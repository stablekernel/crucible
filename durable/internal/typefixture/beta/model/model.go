// Package model is a test fixture: it and the model package under alpha each define a
// Result, so a test can tell two types apart that share the short name model.Result.
package model

// Result is beta's model.Result. Its JSON is alpha's, so a decoder for it decodes an
// alpha Result's JSON without error.
type Result struct {
	ID   int64
	Name string
}
