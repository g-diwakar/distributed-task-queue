package retry

import (
	"math"
	"time"
)

// Exponential backs off with delay = base * 2^(n-1), capped at max.
// Example: base=1s, max=30s → 1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
type Exponential struct {
	Base time.Duration
	Max  time.Duration
}

func NewExponential(base, max time.Duration) *Exponential {
	return &Exponential{Base: base, Max: max}
}

func (e *Exponential) NextDelay(n int) time.Duration {
	delay := time.Duration(float64(e.Base) * math.Pow(2, float64(n-1)))
	if delay > e.Max {
		return e.Max
	}
	return delay
}
