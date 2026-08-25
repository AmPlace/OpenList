package fs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type copyLimiterTestDriver struct {
	driver.Driver
	storage model.Storage
	name    string
}

func (d *copyLimiterTestDriver) Config() driver.Config {
	return driver.Config{Name: d.name}
}

func (d *copyLimiterTestDriver) GetStorage() *model.Storage {
	return &d.storage
}

func TestCopyStorageLimiterWaitsAndReleases(t *testing.T) {
	limiter := newCopyStorageLimiter(1)
	if err := limiter.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		if err := limiter.acquire(context.Background()); err != nil {
			t.Errorf("second acquire failed: %v", err)
			return
		}
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire should wait for the first slot")
	case <-time.After(20 * time.Millisecond):
	}

	limiter.release()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second acquire did not proceed after release")
	}
	limiter.release()
}

func TestCopyStorageLimiterRespectsContext(t *testing.T) {
	limiter := newCopyStorageLimiter(1)
	if err := limiter.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	defer limiter.release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := limiter.acquire(ctx); err == nil {
		t.Fatal("expected acquire to return context error")
	}
}

func TestCopyStorageLimiterReducesLimit(t *testing.T) {
	limiter := newCopyStorageLimiter(10)
	limiter.reduceLimit(3)
	limiter.reduceLimit(5)
	if got := limiter.currentLimit(); got != 3 {
		t.Fatalf("limit = %d, want 3", got)
	}
}

func TestIs115CopyDriver(t *testing.T) {
	for _, name := range []string{"115 Cloud", "115 Open"} {
		if !is115CopyDriver(name) {
			t.Errorf("%q should be recognized as a 115 copy driver", name)
		}
	}
	if is115CopyDriver("189 Cloud") {
		t.Fatal("189 Cloud should not be recognized as a 115 copy driver")
	}
}

func TestIs115PmtError(t *testing.T) {
	pan115 := &copyLimiterTestDriver{name: "115 Cloud"}
	if !is115PmtError(pan115, errors.New("request failed: 115 PMT limit")) {
		t.Fatal("115 PMT error should be recognized")
	}

	non115 := &copyLimiterTestDriver{name: "189 Cloud"}
	if is115PmtError(non115, errors.New("request failed: 115 PMT limit")) {
		t.Fatal("115 PMT error from another driver should be ignored")
	}
}
