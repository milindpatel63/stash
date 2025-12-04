package fsutil

import "sync"

// IOLimiter limits the number of concurrent IO operations per file path.
// It is intentionally simple and self-contained so it can be reverted easily.
type IOLimiter struct {
	mu         sync.Mutex
	cond       *sync.Cond
	perFile    map[string]int
	maxPerFile int
}

// NewIOLimiter creates a new IOLimiter that allows up to maxPerFile
// concurrent IO operations per file path.
func NewIOLimiter(maxPerFile int) *IOLimiter {
	if maxPerFile < 1 {
		maxPerFile = 1
	}

	l := &IOLimiter{
		perFile:    make(map[string]int),
		maxPerFile: maxPerFile,
	}
	l.cond = sync.NewCond(&l.mu)

	return l
}

// Acquire blocks until there is capacity to perform IO on the given path.
func (l *IOLimiter) Acquire(path string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.perFile[path] >= l.maxPerFile {
		l.cond.Wait()
	}

	l.perFile[path]++
}

// Release signals that IO on the given path has completed.
func (l *IOLimiter) Release(path string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if current, ok := l.perFile[path]; ok {
		if current <= 1 {
			delete(l.perFile, path)
		} else {
			l.perFile[path] = current - 1
		}
	}

	l.cond.Broadcast()
}


