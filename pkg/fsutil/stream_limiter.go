package fsutil

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// pendingRequest tracks a pending request waiting for debounce period.
type pendingRequest struct {
	cancel context.CancelFunc
}

// StreamLimiter limits the number of concurrent file streams per file path.
// Uses a blocking semaphore to prevent file opens until a slot is available,
// and debouncing to cancel old requests when new ones arrive rapidly.
type StreamLimiter struct {
	// semaphores: buffered channels acting as semaphores per file path
	semaphores map[string]chan struct{}
	// pendingRequests: tracks pending requests per file path for debouncing
	pendingRequests map[string]*pendingRequest
	mutex           sync.RWMutex
	maxConcurrent   int
	debounceDelay   time.Duration
}

// NewStreamLimiter creates a new StreamLimiter with the specified max concurrent streams per file.
func NewStreamLimiter(maxConcurrent int) *StreamLimiter {
	if maxConcurrent < 1 {
		maxConcurrent = 3 // default to 3 concurrent streams
	}
	return &StreamLimiter{
		semaphores:      make(map[string]chan struct{}),
		pendingRequests: make(map[string]*pendingRequest),
		maxConcurrent:   maxConcurrent,
		debounceDelay:   time.Second, // Wait 1 second for debouncing
	}
}

// getOrCreateSemaphore returns a semaphore channel for the given file path.
func (sl *StreamLimiter) getOrCreateSemaphore(filePath string) chan struct{} {
	sl.mutex.RLock()
	sem, exists := sl.semaphores[filePath]
	sl.mutex.RUnlock()

	if exists {
		return sem
	}

	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	// Double-check after acquiring write lock
	if sem, exists := sl.semaphores[filePath]; exists {
		return sem
	}

	// Create new buffered channel (acts as semaphore)
	sem = make(chan struct{}, sl.maxConcurrent)
	sl.semaphores[filePath] = sem
	return sem
}

// waitForDebounce waits for the debounce period, cancelling old pending requests if a new one arrives.
// Returns true if we should proceed (no new request came), false if cancelled (new request came).
func (sl *StreamLimiter) waitForDebounce(ctx context.Context, filePath string) bool {
	sl.mutex.Lock()

	// Cancel any existing pending request for this file
	if existing, found := sl.pendingRequests[filePath]; found {
		existing.cancel()
	}

	// Create a cancellable context for this pending request
	debounceCtx, cancel := context.WithCancel(ctx)
	sl.pendingRequests[filePath] = &pendingRequest{cancel: cancel}
	sl.mutex.Unlock()

	// Wait for debounce period or cancellation
	timer := time.NewTimer(sl.debounceDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		// Debounce period passed, no new request came
		sl.mutex.Lock()
		delete(sl.pendingRequests, filePath)
		sl.mutex.Unlock()
		return true
	case <-debounceCtx.Done():
		// New request came, we're cancelled
		return false
	case <-ctx.Done():
		// Original context cancelled
		sl.mutex.Lock()
		delete(sl.pendingRequests, filePath)
		sl.mutex.Unlock()
		return false
	}
}

// ServeFileWithLimit serves a file with concurrent stream limiting and debouncing.
// 1. Waits for debounce period (1s) - if new request comes, cancels this one
// 2. Acquires semaphore slot (blocks if all 3 slots are in use)
// 3. Serves the file
// 4. Releases semaphore slot when done
func (sl *StreamLimiter) ServeFileWithLimit(ctx context.Context, w http.ResponseWriter, r *http.Request, filePath string) bool {
	// Step 1: Debounce - wait 1 second, cancel if new request comes
	if !sl.waitForDebounce(ctx, filePath) {
		// We were cancelled by a new request
		return false
	}

	// Step 2: Acquire semaphore slot (blocks if all slots are in use)
	// This prevents file opens until a slot is available
	sem := sl.getOrCreateSemaphore(filePath)

	select {
	case sem <- struct{}{}:
		// Slot acquired, now we can safely open the file
		defer func() {
			// Release slot when done
			select {
			case <-sem:
			default:
				// Channel already empty, shouldn't happen but safe to ignore
			}
		}()

		// Step 3: Serve the file - will return when complete or client disconnects
		http.ServeFile(w, r, filePath)
		return true

	case <-ctx.Done():
		// Context cancelled before we could acquire a slot
		return false
	}
}

