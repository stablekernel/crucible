// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

// TestRun_ParsesFlagsAndDrives parses a full flag set with no reachable broker
// (empty -brokers) so RunKafka fails fast at inlet construction. It exercises the
// flag wiring, config assembly, and the RunKafka call without touching a broker.
func TestRun_ParsesFlagsAndDrives(t *testing.T) {
	args := []string{
		"-brokers", "",
		"-topic", "fulfillment",
		"-group", "g",
		"-client-id", "sourcedrive",
		"-dlq-topic", "fulfillment.DLQ",
	}
	err := run(context.Background(), args, io.Discard, nil)
	if err == nil {
		t.Fatal("run() error = nil, want inlet construction error from empty brokers")
	}
}

// TestRun_FlagParseError returns the flag package's error on an unknown flag,
// covering the parse-failure branch.
func TestRun_FlagParseError(t *testing.T) {
	err := run(context.Background(), []string{"-nope"}, io.Discard, nil)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("run() error = %v, want a flag-parse error", err)
	}
}
