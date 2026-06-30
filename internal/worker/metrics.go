package worker

import "sync/atomic"

// Metrics tracks aggregate job counts across all workers in a pool.
// All methods are safe for concurrent use.
type Metrics struct {
	active    atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
}

func (m *Metrics) incActive()    { m.active.Add(1) }
func (m *Metrics) decActive()    { m.active.Add(-1) }
func (m *Metrics) incCompleted() { m.completed.Add(1) }
func (m *Metrics) incFailed()    { m.failed.Add(1) }

func (m *Metrics) Active() int64    { return m.active.Load() }
func (m *Metrics) Completed() int64 { return m.completed.Load() }
func (m *Metrics) Failed() int64    { return m.failed.Load() }
