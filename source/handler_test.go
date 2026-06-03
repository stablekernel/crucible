// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
)

func TestResultConstructors(t *testing.T) {
	t.Parallel()
	errX := errors.New("x")
	tests := []struct {
		name    string
		result  source.Result
		action  source.Action
		class   source.Classification
		requeue time.Duration
		failed  bool
		hasErr  bool
	}{
		{"Ack", source.Ack(), source.ActionAck, source.Unclassified, 0, false, false},
		{"Nak", source.Nak(errX), source.ActionNak, source.Retryable, 0, true, true},
		{"NakAfter", source.NakAfter(2*time.Second, errX), source.ActionNak, source.Retryable, 2 * time.Second, true, true},
		{"Term", source.Term(errX), source.ActionTerm, source.Poison, 0, true, true},
		{"Reject", source.Reject(errX), source.ActionTerm, source.InvalidForState, 0, true, true},
		{"Skip", source.Skip(), source.ActionAck, source.Drop, 0, false, false},
		{"InProgress", source.InProgress(), source.ActionInProgress, source.Unclassified, 0, false, false},
		{"Manual", source.Manual(), source.ActionManual, source.Unclassified, 0, false, false},
		{"zero is ack", source.Result{}, source.ActionAck, source.Unclassified, 0, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := tt.result
			if r.Action != tt.action {
				t.Errorf("Action = %v, want %v", r.Action, tt.action)
			}
			if r.Class != tt.class {
				t.Errorf("Class = %v, want %v", r.Class, tt.class)
			}
			if r.Requeue != tt.requeue {
				t.Errorf("Requeue = %v, want %v", r.Requeue, tt.requeue)
			}
			if r.Failed() != tt.failed {
				t.Errorf("Failed() = %v, want %v", r.Failed(), tt.failed)
			}
			if (r.Err != nil) != tt.hasErr {
				t.Errorf("Err present = %v, want %v", r.Err != nil, tt.hasErr)
			}
		})
	}
}

func TestActionString(t *testing.T) {
	t.Parallel()
	cases := map[source.Action]string{
		source.ActionAck:        "ack",
		source.ActionNak:        "nak",
		source.ActionTerm:       "term",
		source.ActionInProgress: "in_progress",
		source.ActionManual:     "manual",
		source.Action(99):       "unknown",
	}
	for a, want := range cases {
		if got := a.String(); got != want {
			t.Errorf("Action(%d).String() = %q, want %q", a, got, want)
		}
	}
}

func TestClassificationString(t *testing.T) {
	t.Parallel()
	cases := map[source.Classification]string{
		source.Unclassified:       "unclassified",
		source.Retryable:          "retryable",
		source.Poison:             "poison",
		source.InvalidForState:    "invalid_for_state",
		source.Drop:               "drop",
		source.Classification(99): "unclassified",
	}
	for c, want := range cases {
		if got := c.String(); got != want {
			t.Errorf("Classification(%d).String() = %q, want %q", c, got, want)
		}
	}
}
