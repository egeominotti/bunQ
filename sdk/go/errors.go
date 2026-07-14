package bunqueue

import (
	"errors"
	"fmt"
	"strings"
)

// ConnectionError: the TCP connection failed, closed unexpectedly, or could
// not be established.
type ConnectionError struct{ Message string }

func (e *ConnectionError) Error() string { return e.Message }

// CommandTimeoutError: no response within the command timeout. The socket is
// torn down (half-open guard) and the next call reconnects.
type CommandTimeoutError struct{ Message string }

func (e *CommandTimeoutError) Error() string { return e.Message }

// CommandError: the server answered `ok: false` (validation, not found, ...).
type CommandError struct{ Message string }

func (e *CommandError) Error() string { return e.Message }

// AuthError: authentication was rejected (wrong or missing token).
type AuthError struct{ Message string }

func (e *AuthError) Error() string { return e.Message }

// UnrecoverableError: return (or panic with) this from a processor to skip
// the remaining retries — the job is FAILed with `unrecoverable: true` and
// goes straight to the dead letter queue.
type UnrecoverableError struct{ Message string }

func (e *UnrecoverableError) Error() string { return e.Message }

// NewUnrecoverableError builds an UnrecoverableError with the given message.
func NewUnrecoverableError(message string) *UnrecoverableError {
	return &UnrecoverableError{Message: message}
}

// isNotFound recognizes the server's "not found" answers, which the client
// maps to (nil, nil) instead of an error (wire contract).
func asUnrecoverable(err error, target **UnrecoverableError) bool {
	return errors.As(err, target)
}

func typeName(err error) string {
	return fmt.Sprintf("%T", err)
}

func isNotFound(err error) bool {
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		return false
	}
	return strings.Contains(strings.ToLower(cmdErr.Message), "not found")
}
