// SPDX-License-Identifier: Apache-2.0

//go:build integration

package statsd_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	dogstatsd "github.com/DataDog/datadog-go/v5/statsd"

	csink "github.com/stablekernel/crucible/sink"
	statsdsink "github.com/stablekernel/crucible/sink/statsd"
)

// TestIntegrationAggregatorEmitsToRealUDPListener drives the real Aggregator
// path with a real StatsD SDK client dialed to a live UDP listener on a
// loopback ephemeral port, then reads the datagram back to prove the aggregated
// counter was emitted on Flush.
func TestIntegrationAggregatorEmitsToRealUDPListener(t *testing.T) {
	t.Parallel()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Construct the real SDK client directly with telemetry disabled so the
	// loopback listener captures only the metrics this test emits, and with no
	// buffering window so each flushed metric is written promptly.
	client, err := dogstatsd.New(
		conn.LocalAddr().String(),
		dogstatsd.WithoutTelemetry(),
		dogstatsd.WithBufferFlushInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("dogstatsd.New() error = %v", err)
	}

	// Disable the background loop so Flush is the only emit trigger, keeping the
	// test deterministic.
	agg := statsdsink.NewAggregator(client, statsdsink.WithInterval(0))
	t.Cleanup(func() { _ = agg.(csink.Shutdowner).Shutdown(context.Background()) })

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err = agg.Sink(ctx, statsdsink.Metric{
			Name: "orders.placed",
			Type: statsdsink.TypeCount,
			Int:  1,
		}); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
	}

	if err = agg.(csink.Flusher).Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	// The Aggregator hands the folded counter to the SDK client, which buffers
	// before writing to the socket. Force the SDK to drain to the listener.
	if err = client.Flush(); err != nil {
		t.Fatalf("client.Flush() error = %v", err)
	}

	got := readDatagram(t, conn)
	if !strings.Contains(got, "orders.placed:3|c") {
		t.Fatalf("datagram = %q, want aggregated orders.placed:3|c", got)
	}
}

// readDatagram reads one UDP datagram from conn within a short deadline. The
// StatsD SDK may buffer briefly before flushing to the socket, so it retries
// until the read succeeds or the deadline elapses.
func readDatagram(t *testing.T, conn *net.UDPConn) string {
	t.Helper()
	buf := make([]byte, 4096)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}
		n, _, err := conn.ReadFromUDP(buf)
		if err == nil {
			return string(buf[:n])
		}
		if time.Now().After(deadline) {
			t.Fatalf("ReadFromUDP() timed out without a datagram: %v", err)
		}
	}
}
