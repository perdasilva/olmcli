package eventbus

import (
	"context"
	"sync"
)

// autoClose tracks the event sources that provide input into a destination
// as those input sources finish, they are no longer tracked.
// once an autoClose has run out of active input sources the input channel
// for the destination is closed signalling end of input
type autoClose struct {
	eventSourceID EventSourceID
	sourceSet     map[EventSourceID]struct{}
	lock          sync.RWMutex
}

func (a *autoClose) isOutOfSources() bool {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return len(a.sourceSet) == 0
}

// popSource removes an input source from the tracking set
func (a *autoClose) popSource(eventSourceID EventSourceID) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	if _, ok := a.sourceSet[eventSourceID]; ok {
		delete(a.sourceSet, eventSourceID)
		return true
	}
	return false
}

// pushSource adds an input source to the tracking set
func (a *autoClose) pushSource(eventSourceID EventSourceID) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.sourceSet[eventSourceID] = struct{}{}
}

// autoCloseTable tracks event sources with an active autoClose
type autoCloseTable map[EventSourceID]*autoClose

// autoCloser manages the autoCloseTable
type autoCloser struct {
	autoCloseTable   autoCloseTable
	autoCloseSources map[EventSourceID]struct{}
	router           *router
	lock             sync.RWMutex
}

func newAutoCloser(router *router) *autoCloser {
	return &autoCloser{
		autoCloseTable:   autoCloseTable{},
		autoCloseSources: map[EventSourceID]struct{}{},
		router:           router,
		lock:             sync.RWMutex{},
	}
}

func (s *autoCloser) addAutoCloseSource(source EventSourceID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.autoCloseSources[source] = struct{}{}
}

func (s *autoCloser) deleteAutoClose(eventSourceID EventSourceID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.autoCloseTable, eventSourceID)
}

func (s *autoCloser) addSourceToDestinationAutoClose(src EventSourceID, dests ...EventSourceID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, dest := range dests {
		if _, ok := s.autoCloseTable[dest]; !ok {
			s.autoCloseTable[dest] = &autoClose{
				eventSourceID: dest,
				sourceSet:     map[EventSourceID]struct{}{src: {}},
				lock:          sync.RWMutex{},
			}
		}
		s.autoCloseTable[dest].sourceSet[src] = struct{}{}
	}
}

func (s *autoCloser) hasAutoCloseSource(source EventSourceID) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.autoCloseSources[source]
	return ok
}

func (s *autoCloser) allAutoCloses() []*autoClose {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var out []*autoClose
	for _, ac := range s.autoCloseTable {
		out = append(out, ac)
	}
	return out
}

func (s *autoCloser) AutoCloseDestination(ctx context.Context, src EventSourceID, dests ...EventSourceID) {
	if !s.hasAutoCloseSource(src) {
		conn, ok := s.router.getRouteTo(src)
		if ok {
			go func(src EventSourceID, conn *connection) {
				// fmt.Printf("auto-closer: starting watch on %s\n", src)
				defer func() {
					// pop the finished input source from all destination auto-closes
					for _, dest := range s.allAutoCloses() {
						if dest.popSource(src) {
							// fmt.Printf("auto-close: deleted %s from %s (%d remaining)\n", src, dest.eventSourceID, len(dest.sourceSet))
						}

						// if a destination is out of input sources
						// close the destination input channel to signal end-of-input
						if dest.isOutOfSources() {
							conn, ok := s.router.getRouteTo(dest.eventSourceID)
							if ok {
								// fmt.Printf("%s auto-closed\n", conn.eventSourceID)
								conn.CloseInputChannel()
								s.deleteAutoClose(dest.eventSourceID)
							}
						}
					}
				}()
				// wait for either the context to be done
				// or the input source to be done -> signal the end of output
				connDone := make(chan struct{})
				conn.NotifyOnConnectionDone(connDone)
				select {
				case <-ctx.Done():
					return
				case <-connDone:
					// fmt.Printf("auto-close: %s: source is finished processing\n", src)
					return
				}
			}(src, conn)
			s.addAutoCloseSource(src)
		}
	}
	s.addSourceToDestinationAutoClose(src, dests...)
}
