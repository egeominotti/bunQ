package bunqueue

import (
	"regexp"
	"testing"
)

func TestRandomFlowIDIsCanonicalUniqueUUIDv4(t *testing.T) {
	pattern := regexp.MustCompile(
		`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
	)
	seen := map[string]bool{}
	for range 100 {
		id, err := randomFlowID()
		if err != nil {
			t.Fatalf("generate flow id: %v", err)
		}
		if !pattern.MatchString(id) {
			t.Fatalf("non-canonical flow id: %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate random flow id: %q", id)
		}
		seen[id] = true
	}
}
