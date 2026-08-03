package bunqueue

import "fmt"

// terminalOutcomeApplied distinguishes a normal ACK/FAIL from an authoritative
// no-op after the broker has already finalized the exact lease generation.
func terminalOutcomeApplied(response map[string]any) (bool, error) {
	data, exists := response["data"]
	if !exists || data == nil {
		return true, nil
	}
	payload, ok := data.(map[string]any)
	if !ok {
		return false, fmt.Errorf("invalid terminal outcome response data")
	}
	applied, hasApplied := payload["applied"].(bool)
	if hasApplied && !applied && asString(payload["reason"]) == "already-finalized" {
		return false, nil
	}
	return false, fmt.Errorf("invalid terminal outcome response data")
}
