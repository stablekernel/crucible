# bench-gate.awk — fail the benchmark regression gate when a metric regresses
# past the allowed ratio.
#
# Input:  benchstat CSV (`benchstat -format csv base.txt head.txt`) on stdin.
# Output: a human-readable verdict per regressed benchmark on stdout.
# Exit:   non-zero if any gated metric regressed beyond THRESHOLD.
#
# THRESHOLD is the maximum allowed head/base ratio before the gate fails. It is
# passed in via -v THRESHOLD=<float> and defaults to 1.20 (a 20% regression).
# It is deliberately generous to absorb CI-runner and micro-benchmark jitter on
# the shared GitHub-hosted runners; tighten it once the bench history is stable.
#
# Gated metrics are sec/op (time per op) and allocs/op. B/op is reported by
# benchstat but intentionally NOT gated — allocs/op is the stabler allocation
# signal. New benchmarks (present in head, absent in base) and removed ones are
# skipped, never failed: a missing base or head value yields no ratio.
#
# The benchstat CSV groups results into one table per metric. Each table starts
# with a file-name header row, then a units row (",<unit>,CI,<unit>,CI,vs base,P"),
# then one data row per benchmark: "<name>,<base>,<baseCI>,<head>,<headCI>,<vs>,<P>".
# Column 2 is the base value, column 4 is the head value (1-indexed awk fields).

BEGIN {
    FS = ","
    if (THRESHOLD == "") THRESHOLD = 1.20
    metric = ""
    failed = 0
}

# A units row tells us which metric the following data rows belong to.
$2 == "sec/op"    { metric = "sec/op";    next }
$2 == "allocs/op" { metric = "allocs/op"; next }
$2 == "B/op"      { metric = "B/op";      next }  # tracked but not gated

# Skip non-data rows: blank lines, the goos/goarch/pkg/cpu preamble,
# file-name header rows, and the geomean summary row.
$1 == "" || $1 == "geomean" { next }
NF < 4 { next }

# Only data rows for a known metric reach here.
metric == "" { next }

{
    name = $1
    base = $2 + 0
    head = $4 + 0

    # New or removed benchmark: one side has no number. Never fail on these.
    if ($2 == "" || $4 == "" || base <= 0) next

    # B/op is reported for context but not part of the gate.
    if (metric == "B/op") next

    ratio = head / base
    pct = (ratio - 1) * 100
    if (ratio > THRESHOLD) {
        printf "REGRESSION  %-28s %-10s %+7.1f%%  (%.4g -> %.4g, ratio %.3f > %.2f)\n", \
            name, metric, pct, base, head, ratio, THRESHOLD
        failed = 1
    }
}

END {
    if (failed) {
        printf "\nBenchmark gate FAILED: one or more metrics regressed past %.0f%% (ratio %.2f).\n", \
            (THRESHOLD - 1) * 100, THRESHOLD
        exit 1
    }
    printf "Benchmark gate PASSED: no metric regressed past %.0f%% (ratio %.2f).\n", \
        (THRESHOLD - 1) * 100, THRESHOLD
}
