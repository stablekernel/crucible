package state_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

func TestFireEventData(t *testing.T) {
	tests := []struct {
		name     string
		opts     []state.FireOption
		wantData any
		wantOK   bool
	}{
		{"no options", nil, nil, false},
		{"no payload option", []state.FireOption{state.CollectAll()}, nil, false},
		{"a payload", []state.FireOption{state.WithEventData(3)}, 3, true},
		{"a nil payload is still set", []state.FireOption{state.WithEventData(nil)}, nil, true},
		{"the last payload wins", []state.FireOption{state.WithEventData(1), state.WithEventData(2)}, 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, ok := state.FireEventData(tt.opts...)
			if data != tt.wantData || ok != tt.wantOK {
				t.Fatalf("FireEventData = (%v, %v), want (%v, %v)", data, ok, tt.wantData, tt.wantOK)
			}
		})
	}
}
