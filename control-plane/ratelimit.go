package main

import (
	"sync"
	"time"
)

// ipRateLimiter is a fixed-window per-key counter. One control-plane
// replica only — enough to blunt waitlist spray without Redis.
type ipRateLimiter struct {
	limit  int
	window time.Duration
	mu     sync.Mutex
	counts map[rateKey]int
}

type rateKey struct {
	key    string
	window int64
}

func newIPRateLimiter(limit int, window time.Duration) *ipRateLimiter {
	return &ipRateLimiter{
		limit:  limit,
		window: window,
		counts: make(map[rateKey]int),
	}
}

func (rl *ipRateLimiter) allow(key string) bool {
	if rl.limit <= 0 {
		return true
	}
	now := time.Now()
	w := now.Unix() / int64(rl.window.Seconds())
	k := rateKey{key: key, window: w}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if len(rl.counts) > 20_000 {
		for ck := range rl.counts {
			if ck.window < w-1 {
				delete(rl.counts, ck)
			}
		}
	}
	c := rl.counts[k]
	if c >= rl.limit {
		return false
	}
	rl.counts[k] = c + 1
	return true
}
