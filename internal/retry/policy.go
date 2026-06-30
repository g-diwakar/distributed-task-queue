package retry

import "time"

// Policy determines how long to wait before each retry attempt.
type Policy interface {
	// NextDelay returns the wait duration before attempt number n.
	// n is 1-based: 1 = first retry, 2 = second retry, etc.
	NextDelay(n int) time.Duration
}
