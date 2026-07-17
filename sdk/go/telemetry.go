package bunqueue

import "time"

// TelemetryEvent is emitted for connection lifecycle and command outcomes.
// It never contains command payloads or authentication tokens.
type TelemetryEvent struct {
	Type       string
	Timestamp  time.Time
	Command    string
	Duration   time.Duration
	Generation int
	Error      string
}

// TelemetryCallback receives optional synchronous connection telemetry.
// Callbacks run after the connection mutex is released and must return quickly.
type TelemetryCallback func(TelemetryEvent)

func (c *Connection) queueEventLocked(kind, command string, duration time.Duration, err error) {
	event := TelemetryEvent{
		Type:       kind,
		Timestamp:  time.Now(),
		Command:    command,
		Duration:   duration,
		Generation: c.generation,
	}
	if err != nil {
		event.Error = err.Error()
	}
	c.pendingEvents = append(c.pendingEvents, event)
}

func (c *Connection) takeEventsLocked() []TelemetryEvent {
	events := c.pendingEvents
	c.pendingEvents = nil
	return events
}

func (c *Connection) deliverEvents(events []TelemetryEvent) {
	callback := c.opts.OnEvent
	if callback == nil {
		return
	}
	for _, event := range events {
		func() {
			defer func() { _ = recover() }()
			callback(event)
		}()
	}
}

func commandName(command map[string]any) string {
	name, _ := command["cmd"].(string)
	return name
}
