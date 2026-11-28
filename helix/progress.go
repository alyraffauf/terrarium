package helix

import (
	"sync"
	"time"
)

type recentCompletions struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func (c *recentCompletions) record(did string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == nil {
		c.last = make(map[string]time.Time)
	}
	c.last[did] = now
}

// The progress logger reads this window every minute even without a scraper.
func (c *recentCompletions) count(now time.Time) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := now.Add(-time.Minute)
	for did, last := range c.last {
		if !last.After(cutoff) {
			delete(c.last, did)
		}
	}
	return int64(len(c.last))
}
