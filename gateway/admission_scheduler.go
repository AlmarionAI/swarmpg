package main

import (
	"context"
	"sync"
)

type admissionClass string

const (
	admissionGeneral admissionClass = "general"
	admissionControl admissionClass = "control"
)

type admissionWaiter struct {
	org     string
	ready   chan struct{}
	started bool
}

type admissionLane struct {
	capacity, active, queued int
	order                    []string
	queues                   map[string][]*admissionWaiter
}

// Queued organizations rotate after every grant. General work never borrows
// control capacity, and no shared database permit is held while queued here.
type admissionScheduler struct {
	mu                           sync.Mutex
	lanes                        map[admissionClass]*admissionLane
	orgQueued                    map[string]int
	queued, orgLimit, totalLimit int
	closed                       bool
}

func newAdmissionScheduler(general, control, orgLimit, totalLimit int) *admissionScheduler {
	return &admissionScheduler{lanes: map[admissionClass]*admissionLane{
		admissionGeneral: {capacity: general, queues: map[string][]*admissionWaiter{}},
		admissionControl: {capacity: control, queues: map[string][]*admissionWaiter{}},
	}, orgQueued: map[string]int{}, orgLimit: orgLimit, totalLimit: totalLimit}
}

func (s *admissionScheduler) dispatch(l *admissionLane) {
	for !s.closed && l.active < l.capacity && len(l.order) > 0 {
		org := l.order[0]
		l.order = l.order[1:]
		q := l.queues[org]
		w := q[0]
		if len(q) == 1 {
			delete(l.queues, org)
		} else {
			l.queues[org] = q[1:]
			l.order = append(l.order, org)
		}
		l.queued--
		s.queued--
		s.orgQueued[org]--
		if s.orgQueued[org] == 0 {
			delete(s.orgQueued, org)
		}
		w.started = true
		l.active++
		close(w.ready)
	}
}

func (s *admissionScheduler) remove(l *admissionLane, w *admissionWaiter) {
	q := l.queues[w.org]
	for i, candidate := range q {
		if candidate != w {
			continue
		}
		q = append(q[:i], q[i+1:]...)
		l.queued--
		s.queued--
		s.orgQueued[w.org]--
		if s.orgQueued[w.org] == 0 {
			delete(s.orgQueued, w.org)
		}
		if len(q) > 0 {
			l.queues[w.org] = q
		} else {
			delete(l.queues, w.org)
			for j, org := range l.order {
				if org == w.org {
					l.order = append(l.order[:j], l.order[j+1:]...)
					break
				}
			}
		}
		return
	}
}

func (s *admissionScheduler) acquire(ctx context.Context, org string, class admissionClass) (func(), *apiError) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, admissionUnavailable()
	}
	l := s.lanes[class]
	if l == nil {
		s.mu.Unlock()
		return nil, admissionUnavailable()
	}
	if ctx.Err() != nil {
		s.mu.Unlock()
		return nil, admissionRejected("local_queue_timeout", 100)
	}
	// Empty slots can be used immediately without consuming queue capacity.
	if l.active < l.capacity && l.queued == 0 {
		l.active++
		s.mu.Unlock()
		return s.release(l), nil
	}
	laneLimit := max(1, s.totalLimit*3/4)
	if class == admissionControl {
		laneLimit = max(1, s.totalLimit-laneLimit)
	}
	if s.queued >= s.totalLimit || l.queued >= laneLimit || len(l.queues[org]) >= s.orgLimit {
		s.mu.Unlock()
		return nil, admissionRejected("local_queue_full", 100)
	}
	w := &admissionWaiter{org: org, ready: make(chan struct{})}
	if len(l.queues[org]) == 0 {
		l.order = append(l.order, org)
	}
	l.queues[org] = append(l.queues[org], w)
	l.queued++
	s.queued++
	s.orgQueued[org]++
	s.mu.Unlock()
	select {
	case <-w.ready:
	case <-ctx.Done():
	}
	s.mu.Lock()
	if ctx.Err() != nil || s.closed {
		if w.started {
			l.active--
		} else {
			s.remove(l, w)
		}
		s.dispatch(l)
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return nil, admissionUnavailable()
		}
		return nil, admissionRejected("local_queue_timeout", 100)
	}
	s.mu.Unlock()
	return s.release(l), nil
}

func (s *admissionScheduler) release(l *admissionLane) func() {
	var once sync.Once
	return func() { once.Do(func() { s.mu.Lock(); l.active--; s.dispatch(l); s.mu.Unlock() }) }
}

func (s *admissionScheduler) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, l := range s.lanes {
		for _, q := range l.queues {
			for _, w := range q {
				close(w.ready)
			}
		}
	}
}

func (s *admissionScheduler) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{}
	for class, l := range s.lanes {
		out[string(class)] = map[string]int{"active": l.active, "queued": l.queued}
	}
	return out
}
