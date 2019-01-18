// This package is based on github.com/lightningnetwork/lnd/multimutex.
package multimutex

import (
	"fmt"
	"sync"
)

// countMutex wraps a Mutex with a count of the number of goroutines waiting on
// this mutex.
type countMutex struct {
	count int
	sync.Mutex
}

// KeyedMutex provides a way of synchronizing access to data keyed by a name. If
// a goroutine holds a named mutex then any other goroutine trying to access the
// same resource will block until the resource is available.
type KeyedMutex struct {
	mutexes map[string]*countMutex

	// mapMutex synchronizes access to the mutexes map.
	mapMutex sync.Mutex
}

func NewKeyedMutex() *KeyedMutex {
	return &KeyedMutex{
		mutexes: make(map[string]*countMutex),
	}
}

// Lock locks the mutex with the given name. If the mutex is already
// locked by, Lock blocks until the mutex is available.
func (c *KeyedMutex) Lock(name string) {
	c.mapMutex.Lock()
	mtx, ok := c.mutexes[name]
	if ok {
		// If the mutex already existed in the map, we
		// increment its counter, to indicate that there
		// now is one more goroutine waiting for it.
		mtx.count++
	} else {
		// If it was not in the map, it means no other
		// goroutine has locked the mutex for this name,
		// and we can create a new mutex with count 1
		// and add it to the map.
		mtx = &countMutex{
			count: 1,
		}
		c.mutexes[name] = mtx
	}
	c.mapMutex.Unlock()

	// Acquire the mutex for this name.
	mtx.Lock()
}

// Unlock unlocks the mutex with the given name. It is a run-time
// error if the mutex is not locked by the name on entry to Unlock.
func (c *KeyedMutex) Unlock(name string) {
	c.mapMutex.Lock()

	mtx, ok := c.mutexes[name]
	if !ok {
		// The mutex not existing in the map means
		// an unlock for a name not currently locked
		// was attempted.
		panic(fmt.Sprintf("double unlock for %s", name))
	}

	// Decrement the counter. If the count goes to zero, it means this caller
	// was the last one to wait for the mutex, and we can delete it from the
	// map. We can do this safely since we are under the mapMutex, meaning
	// that all other goroutines waiting for the mutex already have incremented
	// it, or will create a new mutex when they get the mapMutex.
	mtx.count--
	if mtx.count == 0 {
		delete(c.mutexes, name)
	}
	c.mapMutex.Unlock()

	// Unlock the mutex for this name.
	mtx.Unlock()
}
