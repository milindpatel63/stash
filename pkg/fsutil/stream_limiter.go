package fsutil

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// activeStream tracks an active stream with its context and start time.
type activeStream struct {
	ctx       context.Context
	cancel    context.CancelFunc
	startTime time.Time
}

// pendingRequest tracks a pending request waiting for debounce period.
type pendingRequest struct {
	cancel context.CancelFunc
}

// StreamLimiter limits the number of concurrent file streams per file path.
// When at limit, new requests immediately cancel the oldest active stream.
// Also uses debouncing to handle rapid seeks.
type StreamLimiter struct {
	// activeStreams: tracks currently active streams per file path
	activeStreams map[string][]*activeStream
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
		activeStreams:   make(map[string][]*activeStream),
		pendingRequests: make(map[string]*pendingRequest),
		maxConcurrent:   maxConcurrent,
		debounceDelay:   time.Second, // Wait 1 second for debouncing
	}
}

// cancelOldestActiveStream cancels the oldest active stream for the given file path.
// Must be called with write lock held.
func (sl *StreamLimiter) cancelOldestActiveStream(filePath string) {
	streams := sl.activeStreams[filePath]
	if len(streams) == 0 {
		return
	}

	// Find the oldest stream (earliest startTime)
	oldestIdx := 0
	oldestTime := streams[0].startTime
	for i, stream := range streams {
		if stream.startTime.Before(oldestTime) {
			oldestTime = stream.startTime
			oldestIdx = i
		}
	}

	// Cancel the oldest stream immediately - this stops its file I/O
	oldest := streams[oldestIdx]
	oldest.cancel()

	// Remove it from the list
	sl.activeStreams[filePath] = append(streams[:oldestIdx], streams[oldestIdx+1:]...)
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

// registerActiveStream registers a stream as active and returns its context and cleanup function.
func (sl *StreamLimiter) registerActiveStream(ctx context.Context, filePath string) (streamCtx context.Context, cleanup func()) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	// Create a cancellable context for this stream
	cancelCtx, cancel := context.WithCancel(ctx)

	stream := &activeStream{
		ctx:       cancelCtx,
		cancel:    cancel,
		startTime: time.Now(),
	}

	streams := sl.activeStreams[filePath]

	// If we're at the limit, cancel the oldest stream immediately
	if len(streams) >= sl.maxConcurrent {
		sl.cancelOldestActiveStream(filePath)
		// Re-fetch after cancellation
		streams = sl.activeStreams[filePath]
	}

	// Add the new stream
	sl.activeStreams[filePath] = append(streams, stream)

	// Return the stream context and cleanup function
	return cancelCtx, func() {
		sl.mutex.Lock()
		defer sl.mutex.Unlock()

		// Remove this stream from the list
		streams := sl.activeStreams[filePath]
		for i, s := range streams {
			if s == stream {
				sl.activeStreams[filePath] = append(streams[:i], streams[i+1:]...)
				break
			}
		}

		// Clean up empty entries
		if len(sl.activeStreams[filePath]) == 0 {
			delete(sl.activeStreams, filePath)
		}
	}
}

// ServeFileWithLimit serves a file with concurrent stream limiting and debouncing.
// 1. If at limit, immediately cancel oldest stream (skip debounce for responsiveness)
// 2. If not at limit, wait for debounce period (1s) - helps with rapid seeks
// 3. Register as active stream
// 4. Serve the file
// 5. Unregister when done
func (sl *StreamLimiter) ServeFileWithLimit(ctx context.Context, w http.ResponseWriter, r *http.Request, filePath string) bool {
	sl.mutex.RLock()
	activeCount := len(sl.activeStreams[filePath])
	sl.mutex.RUnlock()

	// Step 1: If we're at limit, skip debounce and immediately cancel oldest
	// This ensures external apps don't get stuck waiting
	if activeCount >= sl.maxConcurrent {
		// Cancel oldest immediately to make room
		sl.mutex.Lock()
		sl.cancelOldestActiveStream(filePath)
		sl.mutex.Unlock()
		// Proceed directly to registration (no debounce when at limit)
	} else {
		// Step 1b: Not at limit, use debouncing to handle rapid seeks
		// Wait 1 second, cancel if new request comes
		if !sl.waitForDebounce(ctx, filePath) {
			// We were cancelled by a new request
			return false
		}
	}

	// Step 2: Register as active stream
	// If we're at limit (shouldn't happen after cancelling oldest, but handle it)
	streamCtx, cleanup := sl.registerActiveStream(ctx, filePath)
	defer cleanup()

	// Check if our context was cancelled
	select {
	case <-streamCtx.Done():
		return false
	default:
	}

	// Step 3: Serve the file - will return when complete or context is cancelled
	http.ServeFile(w, r.WithContext(streamCtx), filePath)
	return true
}

