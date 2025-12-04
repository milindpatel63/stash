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

// StreamLimiter limits the number of concurrent file streams per file path.
// When the limit is reached, the oldest stream is cancelled to make room for new ones.
// This prevents accumulation of file handles when external apps keep opening new streams.
type StreamLimiter struct {
	// activeStreams tracks active streams per file path
	activeStreams map[string][]*activeStream
	mutex         sync.RWMutex
	maxConcurrent int
}

// NewStreamLimiter creates a new StreamLimiter with the specified max concurrent streams per file.
func NewStreamLimiter(maxConcurrent int) *StreamLimiter {
	if maxConcurrent < 1 {
		maxConcurrent = 3 // default to 3 concurrent streams
	}
	return &StreamLimiter{
		activeStreams: make(map[string][]*activeStream),
		maxConcurrent: maxConcurrent,
	}
}

// cancelOldestStream cancels the oldest active stream for the given file path.
// Must be called with write lock held.
func (sl *StreamLimiter) cancelOldestStream(filePath string) {
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

	// Cancel the oldest stream
	oldest := streams[oldestIdx]
	oldest.cancel()

	// Remove it from the list
	sl.activeStreams[filePath] = append(streams[:oldestIdx], streams[oldestIdx+1:]...)
}

// registerStream registers an active stream and returns its context and cleanup function.
// If the limit is reached, the oldest stream is cancelled first.
func (sl *StreamLimiter) registerStream(ctx context.Context, filePath string) (streamCtx context.Context, cleanup func()) {
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

	// If we're at the limit, cancel the oldest stream
	if len(streams) >= sl.maxConcurrent {
		sl.cancelOldestStream(filePath)
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

// ServeFileWithLimit serves a file with concurrent stream limiting.
// When the limit is reached, the oldest active stream is cancelled to make room for the new one.
// This ensures external apps don't accumulate file handles by continuously opening new streams.
func (sl *StreamLimiter) ServeFileWithLimit(ctx context.Context, w http.ResponseWriter, r *http.Request, filePath string) bool {
	// Register this stream (will cancel oldest if at limit)
	// Returns the stream's context and cleanup function
	streamCtx, cleanup := sl.registerStream(ctx, filePath)
	defer cleanup()

	// Check if our context was cancelled (shouldn't happen immediately, but check anyway)
	select {
	case <-streamCtx.Done():
		return false
	default:
	}

	// Serve the file - will return when complete or context is cancelled
	http.ServeFile(w, r.WithContext(streamCtx), filePath)
	return true
}

