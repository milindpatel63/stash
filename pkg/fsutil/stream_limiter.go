package fsutil

import (
	"context"
	"net/http"
	"sync"
)

// StreamLimiter limits the number of concurrent file streams per file path.
// This prevents accumulation of file handles when browsers make multiple
// concurrent range requests during video seeking.
type StreamLimiter struct {
	limiters map[string]chan struct{}
	mutex    sync.RWMutex
	maxConcurrent int
}

// NewStreamLimiter creates a new StreamLimiter with the specified max concurrent streams per file.
func NewStreamLimiter(maxConcurrent int) *StreamLimiter {
	if maxConcurrent < 1 {
		maxConcurrent = 3 // default to 3 concurrent streams
	}
	return &StreamLimiter{
		limiters:      make(map[string]chan struct{}),
		maxConcurrent: maxConcurrent,
	}
}

// getOrCreateLimiter returns a semaphore channel for the given file path.
func (sl *StreamLimiter) getOrCreateLimiter(filePath string) chan struct{} {
	sl.mutex.RLock()
	limiter, exists := sl.limiters[filePath]
	sl.mutex.RUnlock()

	if exists {
		return limiter
	}

	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists := sl.limiters[filePath]; exists {
		return limiter
	}

	// Create new buffered channel (acts as semaphore)
	limiter = make(chan struct{}, sl.maxConcurrent)
	sl.limiters[filePath] = limiter
	return limiter
}

// Acquire attempts to acquire a stream slot for the given file path.
// Returns true if acquired, false if context is cancelled or limit reached.
func (sl *StreamLimiter) Acquire(ctx context.Context, filePath string) bool {
	limiter := sl.getOrCreateLimiter(filePath)

	select {
	case limiter <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// Release releases a stream slot for the given file path.
func (sl *StreamLimiter) Release(filePath string) {
	sl.mutex.RLock()
	limiter, exists := sl.limiters[filePath]
	sl.mutex.RUnlock()

	if exists {
		select {
		case <-limiter:
		default:
			// Channel already empty, shouldn't happen but safe to ignore
		}
	}
}

// ServeFileWithLimit serves a file with concurrent stream limiting.
// It acquires a stream slot, serves the file, then releases the slot.
// The slot is released when the file serving completes OR when the request context is cancelled
// (e.g., when external apps disconnect), ensuring slots are always freed.
func (sl *StreamLimiter) ServeFileWithLimit(ctx context.Context, w http.ResponseWriter, r *http.Request, filePath string) bool {
	if !sl.Acquire(ctx, filePath) {
		return false
	}

	// Use sync.Once to ensure we only release once
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			sl.Release(filePath)
		})
	}

	// Always release on function exit
	defer release()

	// Monitor context cancellation in a goroutine to release slot immediately
	// when client disconnects (important for external apps)
	go func() {
		<-ctx.Done()
		release()
	}()

	// Serve the file - this will return when complete or client disconnects
	http.ServeFile(w, r, filePath)
	return true
}

