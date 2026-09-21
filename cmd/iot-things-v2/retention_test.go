package main

import "testing"

// K4: HISTORY_RETENTION_DAYS tolkning — endast positivt heltal aktiverar
// radering, allt annat betyder obegränsat (B6-default).
func TestRetentionDays(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{"30", 30},
		{"1", 1},
		{"0", 0},
		{"", 0},
		{"-5", 0},
		{"abc", 0},
		{"1.5", 0},
		{" 30", 0},
	} {
		if got := retentionDays(flagMap{historyRetentionDays: tc.in}); got != tc.want {
			t.Fatalf("retentionDays(%q)=%d, want %d", tc.in, got, tc.want)
		}
	}
}
