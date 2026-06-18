// Package render emits D2 source from a viewmodel and renders it to SVG.
package render

import (
	"encoding/json"
	"fmt"
	"os"
)

// Theme holds all color values for the forge v5 look.
type Theme struct {
	Bg          string `json:"bg"`
	Steel       string `json:"steel"`
	SteelDark   string `json:"steelDark"`
	Ember       string `json:"ember"`
	Hot         string `json:"hot"`
	Copper      string `json:"copper"`
	HotBright   string `json:"hotBright"`
	TextWarm    string `json:"textWarm"`
	ScaleGrey   string `json:"scaleGrey"`
	ScaleText   string `json:"scaleText"`
	SoftOrange  string `json:"softOrange"`
	InvokeFill  string `json:"invokeFill"`
	InvokeText  string `json:"invokeText"`
	HistoryText string `json:"historyText"`
	DimNodeFill string `json:"dimNodeFill"`
	FaultStroke string `json:"faultStroke"`
	// Theme override slots
	TextSecondary string `json:"textSecondary"` // N2 #cdc4b6
	CanvasN4      string `json:"canvasN4"`      // N4 #3a3631
	CanvasN5      string `json:"canvasN5"`      // N5 #272320
	CanvasN6      string `json:"canvasN6"`      // N6 #1d1a16
	AccentAA4     string `json:"accentAA4"`     // AA4 #2e2418
	AccentAA5     string `json:"accentAA5"`     // AA5 #221a10
	AccentAB4     string `json:"accentAB4"`     // AB4 #2a1606
	AccentAB5     string `json:"accentAB5"`     // AB5 #1c0f04
}

// DefaultTheme is the locked forge v5 palette. Every color is copied verbatim
// from the forge v5 reference so the emitter and renderer reproduce that look.
var DefaultTheme = Theme{
	Bg:            "#161310",
	Steel:         "#474c52",
	SteelDark:     "#33373c",
	Ember:         "#d9620a",
	Hot:           "#ff7a18",
	Copper:        "#c0763a",
	HotBright:     "#ffae42",
	TextWarm:      "#efe7db",
	ScaleGrey:     "#6b7177",
	ScaleText:     "#8d9298",
	SoftOrange:    "#e7c9a6",
	InvokeFill:    "#2a1606",
	InvokeText:    "#f2c39b",
	HistoryText:   "#241405",
	DimNodeFill:   "#22262b",
	FaultStroke:   "#7a341a",
	TextSecondary: "#cdc4b6",
	CanvasN4:      "#3a3631",
	CanvasN5:      "#272320",
	CanvasN6:      "#1d1a16",
	AccentAA4:     "#2e2418",
	AccentAA5:     "#221a10",
	AccentAB4:     "#2a1606",
	AccentAB5:     "#1c0f04",
}

// LoadTheme reads a JSON file and overlays it onto a copy of DefaultTheme, so a
// partial document keeps the default for any field it does not set. It returns
// an error when the file cannot be read or contains invalid JSON.
func LoadTheme(path string) (Theme, error) {
	t := DefaultTheme           // copy: unspecified fields keep their defaults
	b, err := os.ReadFile(path) // #nosec G304 -- path is operator-supplied config
	if err != nil {
		return Theme{}, fmt.Errorf("read theme %q: %w", path, err)
	}
	if err := json.Unmarshal(b, &t); err != nil {
		return Theme{}, fmt.Errorf("parse theme %q: %w", path, err)
	}
	return t, nil
}
