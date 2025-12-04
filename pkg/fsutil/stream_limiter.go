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
// Waits for a slot to become available if all slots are currently in use.
// Returns true if acquired, false if context is cancelled.
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
// It waits for a slot if needed, serves the file, then releases the slot.
// Works for both Stash UI and external apps - slots are released when http.ServeFile returns.
func (sl *StreamLimiter) ServeFileWithLimit(ctx context.Context, w http.ResponseWriter, r *http.Request, filePath string) bool {
	limiter := sl.getOrCreateLimiter(filePath)

	// Wait for a slot to become available (will block if all slots are in use)
	// This ensures we respect the limit while allowing waiting requests to proceed
	// when slots are freed (either by completion or timeout)
	select {
	case limiter <- struct{}{}:
		// Slot acquired, serve the file
		defer func() {
			// Release slot when done (either on completion or client disconnect)
			select {
			case <-limiter:
			default:
				// Channel already empty, shouldn't happen but safe to ignore
			}
		}()
		http.ServeFile(w, r, filePath)
		return true
	case <-ctx.Done():
		// Context cancelled before we could acquire a slot
		return false
	}
}

