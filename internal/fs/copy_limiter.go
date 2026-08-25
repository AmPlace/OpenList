package fs

import (
	"context"
	"strings"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

const (
	pan115CopyLimit    = 10
	pan115PmtCopyLimit = 3
	pan115PmtKeyword   = "115 pmt"
)

type copyStorageLimiter struct {
	mu      sync.Mutex
	limit   int
	active  int
	changed chan struct{}
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
	close(l.changed)
	l.changed = make(chan struct{})
	l.mu.Unlock()
}

func (l *copyStorageLimiter) reduceLimit(limit int) {
	if limit <= 0 {
		return
	}
	l.mu.Lock()
	if limit < l.limit {
		l.limit = limit
		close(l.changed)
		l.changed = make(chan struct{})
	}
	l.mu.Unlock()
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
		pan115CopyLimiters.get(t.SrcStorage).reduceLimit(pan115PmtCopyLimit)
	}
}
