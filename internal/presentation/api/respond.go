package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// FormatETag renders a revision as an HTTP ETag value.
func FormatETag(revision int64) string {
	return fmt.Sprintf(`"rev-%d"`, revision)
}

// ParseETag parses "rev-<n>" with or without quotes (and an optional W/ prefix).
func ParseETag(v string) (int64, error) {
	s := strings.TrimSpace(v)
	s = strings.TrimPrefix(s, "W/")
	s = strings.Trim(s, `"`)
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "rev-") {
		return 0, fmt.Errorf("invalid ETag %q: want rev-<n>", v)
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(s, "rev-"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid ETag %q: %w", v, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid ETag %q: negative revision", v)
	}
	return n, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}
