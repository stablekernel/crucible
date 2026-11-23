// Package model is a test fixture: it and the model package under beta each define a
// Result, so a test can tell two types apart that share the short name model.Result.
package model

// Result is alpha's model.Result.
type Result struct {
	ID   int64
	Name string
}
