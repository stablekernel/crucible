package expr

import "time"

// parseGoDuration parses a Go duration string (e.g. "1500ms") into a time.Duration,
// matching the wire form crucible's ContextSchema uses for a duration field.
func parseGoDuration(s string) (time.Duration, error) {
	return time.ParseDuration(s)
}

// parseRFC3339 parses an RFC 3339 timestamp string into a time.Time, matching the
// wire form crucible's ContextSchema uses for a time field.
func parseRFC3339(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}
