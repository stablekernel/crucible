package durable

import (
	"encoding/json"
	"testing"
)

// TestCheckFormat_RefusesAHigherFormat pins what a downgrade meets. checkFormat fails
// on a Record of a lower format than its checkpoint's, a Store that lost the field, and
// accepts a higher one, as every version since Record.Format has. checkFormats holds
// each Record to the highest format before it, the checkpoint's or a Record's, so an
// older version's Record is refused anywhere in the tail. checkNotNewer, which
// every reader calls first, refuses a checkpoint or a Record of a format above
// journalFormat, one a newer version wrote, so each later bump of journalFormat makes
// this version refuse that version's journal. A binary built before checkNotNewer
// replays a newer journal as its own format.
func TestCheckFormat_RefusesAHigherFormat(t *testing.T) {
	if err := checkFormat(1, &Record{Format: 2}); err != nil {
		t.Errorf("checkFormat(1, a Record of format 2) = %v, want nil", err)
	}
	if err := checkFormat(2, &Record{Format: 1}); err == nil {
		t.Errorf("checkFormat(2, a Record of format 1) = nil, want the older-version error")
	}
	for _, tc := range []struct {
		checkpoint int
		recs       []Record
		refused    bool
	}{
		{0, []Record{{Format: 0}, {Format: 1}, {Format: 1}, {Format: 2}}, false},
		{1, []Record{{Format: 1}, {Format: 2}, {Format: 2}}, false},
		{1, []Record{{Format: 2}, {Format: 1}}, true},
		{0, []Record{{Format: 1}, {Format: 0}}, true},
		{2, []Record{{Format: 1}}, true},
	} {
		err := checkFormats(tc.checkpoint, tc.recs)
		if (err != nil) != tc.refused {
			t.Errorf("checkFormats(%d, %+v) = %v, want refused %v", tc.checkpoint, tc.recs, err, tc.refused)
		}
	}
	for _, tc := range []struct {
		checkpoint int
		recs       []Record
		refused    bool
	}{
		{0, nil, false},
		{journalFormat, nil, false},
		{0, []Record{{Format: 0}, {Format: 1}, {Format: journalFormat}}, false},
		{journalFormat + 1, nil, true},
		{journalFormat, []Record{{Format: journalFormat}, {Format: journalFormat + 1}}, true},
		{0, []Record{{Format: journalFormat + 5}}, true},
	} {
		err := checkNotNewer("x", tc.checkpoint, tc.recs)
		if (err != nil) != tc.refused {
			t.Errorf("checkNotNewer(%d, %+v) = %v, want refused %v", tc.checkpoint, tc.recs, err, tc.refused)
		}
	}
}

// TestParkedEntry_AnOlderReaderSettlesIt pins what a version from before parking reads
// in a parked entry: its serviceResultPayload has only Result and Error, so the entry
// decodes as a result that settled, and its replay would settle the parked result
// through onDone, running the Assign the park held back. The CHANGELOG says not to
// downgrade a Store this version has written to; this is one reason.
func TestParkedEntry_AnOlderReaderSettlesIt(t *testing.T) {
	payload, err := json.Marshal(serviceResultPayload{Result: json.RawMessage(`{"ID":7}`), Parked: true, Type: "x.T"})
	if err != nil {
		t.Fatal(err)
	}
	var older struct {
		Result json.RawMessage `json:"result,omitempty"`
		Error  *string         `json:"error,omitempty"`
	}
	if err := json.Unmarshal(payload, &older); err != nil {
		t.Fatalf("an older reader fails on a parked entry: %v", err)
	}
	if older.Error != nil || string(older.Result) != `{"ID":7}` {
		t.Fatalf("an older reader reads %+v, want a settled result", older)
	}
}
