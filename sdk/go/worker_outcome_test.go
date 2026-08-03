package bunqueue

import "testing"

func TestTerminalOutcomeApplied(t *testing.T) {
	tests := []struct {
		name     string
		response map[string]any
		applied  bool
		wantErr  bool
	}{
		{name: "historical response", response: map[string]any{}, applied: true},
		{
			name: "already finalized",
			response: map[string]any{"data": map[string]any{
				"applied": false, "reason": "already-finalized",
			}},
		},
		{name: "malformed payload", response: map[string]any{"data": "invalid"}, wantErr: true},
		{
			name: "unknown reason",
			response: map[string]any{"data": map[string]any{
				"applied": false, "reason": "unknown",
			}},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			applied, err := terminalOutcomeApplied(test.response)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
			if applied != test.applied {
				t.Fatalf("applied = %v, want %v", applied, test.applied)
			}
		})
	}
}
