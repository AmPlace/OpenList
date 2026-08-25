package fs

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

const (
	pan115CopyLimit         = 10
	pan115PmtCopyLimit      = 3
	pan115CopyProbeInterval = time.Hour
	pan115PmtKeyword        = "115 pmt"
)

var pan115CopyLimitSteps = [...]int{1, 3, 5, 6, 10}

type copyStorageLimiter struct {
	mu          sync.Mutex
	limit       int
	active      int
	changed     chan struct{}
	probing     bool
	probeFrom   int
	nextProbeAt time.Time
}

func newCopyStorageLimiter(limit int) *copyStorageLimiter {
	return &copyStorageLimiter{
		limit:   limit,
		changed: make(chan struct{}),
	}
}

func (l *copyStorageLimiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		l.maybeProbeLocked(time.Now())
		if l.active < l.limit {
			l.active++
			l.mu.Unlock()
			return nil
		}
		changed := l.changed
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (l *copyStorageLimiter) release() {
	l.mu.Lock()
	if l.active > 0 {
		l.active--
	}
	l.notifyLocked()
	l.mu.Unlock()
}

func (l *copyStorageLimiter) onPmt(now time.Time) {
	l.mu.Lock()
	fallback := previous115CopyLimit(l.limit)
	if l.probing && l.probeFrom > 0 {
		fallback = l.probeFrom
	}
	l.probing = false
	l.probeFrom = 0
	l.nextProbeAt = now.Add(pan115CopyProbeInterval)
	if fallback != l.limit {
		l.limit = fallback
		l.notifyLocked()
	}
	l.mu.Unlock()
}

func (l *copyStorageLimiter) maybeProbe(now time.Time) {
	l.mu.Lock()
	l.maybeProbeLocked(now)
	l.mu.Unlock()
}

func (l *copyStorageLimiter) maybeProbeLocked(now time.Time) {
	if l.probing {
		if now.Before(l.nextProbeAt) {
			return
		}
		l.probing = false
		l.probeFrom = 0
		if l.limit >= pan115CopyLimit {
			l.nextProbeAt = time.Time{}
			return
		}
		// The previous probe was stable for the whole interval. Continue
		// climbing one step at a time without waiting another interval.
		l.nextProbeAt = now
	}

	if l.nextProbeAt.IsZero() || now.Before(l.nextProbeAt) || l.limit >= pan115CopyLimit {
		return
	}
	next := next115CopyLimit(l.limit)
	if next <= l.limit {
		l.nextProbeAt = time.Time{}
		return
	}
	l.probeFrom = l.limit
	l.limit = next
	l.probing = true
	l.nextProbeAt = now.Add(pan115CopyProbeInterval)
	l.notifyLocked()
}

func (l *copyStorageLimiter) notifyLocked() {
	close(l.changed)
	l.changed = make(chan struct{})
}

func next115CopyLimit(current int) int {
	for _, limit := range pan115CopyLimitSteps {
		if limit > current {
			return limit
		}
	}
	return current
}

func previous115CopyLimit(current int) int {
	if current >= pan115CopyLimit {
		return pan115PmtCopyLimit
	}
	for i, limit := range pan115CopyLimitSteps {
		if limit == current {
			if i == 0 {
				return current
			}
			return pan115CopyLimitSteps[i-1]
		}
	}
	if current > pan115PmtCopyLimit {
		return pan115PmtCopyLimit
	}
	return 1
}

func (l *copyStorageLimiter) currentLimit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

type copyStorageLimiterRegistry struct {
	mu       sync.Mutex
	limiters map[string]*copyStorageLimiter
}

func (r *copyStorageLimiterRegistry) get(storage driver.Driver) *copyStorageLimiter {
	key := storage.GetStorage().MountPath
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.limiters == nil {
		r.limiters = make(map[string]*copyStorageLimiter)
	}
	if limiter, ok := r.limiters[key]; ok {
		return limiter
	}
	limiter := newCopyStorageLimiter(pan115CopyLimit)
	r.limiters[key] = limiter
	return limiter
}

var pan115CopyLimiters copyStorageLimiterRegistry

func is115CopyDriver(name string) bool {
	return name == "115 Cloud" || name == "115 Open"
}

func is115CopySource(storage driver.Driver) bool {
	return storage != nil && is115CopyDriver(storage.Config().Name)
}

func is115PmtError(storage driver.Driver, err error) bool {
	return is115CopySource(storage) && err != nil && strings.Contains(strings.ToLower(err.Error()), pan115PmtKeyword)
}

func (t *FileTransferTask) observe115CopyError(err error) {
	if is115PmtError(t.SrcStorage, err) {
		pan115CopyLimiters.get(t.SrcStorage).onPmt(time.Now())
	}
}
