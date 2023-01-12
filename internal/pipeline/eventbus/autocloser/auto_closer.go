package autocloser

import (
	"context"
	"sync"

	"github.com/perdasilva/olmcli/internal/pipeline"
	eventrouter2 "github.com/perdasilva/olmcli/internal/pipeline/eventbus/eventrouter"
)

// autoClose tracks the event sources that provide input into a destination
// as those input sources finish, they are no longer tracked.
// once an autoClose has run out of active input sources the input channel
// for the destination is closed signalling end of input
type autoClose struct {
	eventSourceID pipeline.EventSourceID
	sourceSet     map[pipeline.EventSourceID]struct{}
	lock          sync.RWMutex
}

func (a *autoClose) isOutOfSources() bool {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return len(a.sourceSet) == 0
}

// popSource removes an input source from the tracking set
func (a *autoClose) popSource(eventSourceID pipeline.EventSourceID) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	if _, ok := a.sourceSet[eventSourceID]; ok {
		delete(a.sourceSet, eventSourceID)
		return true
	}
	return false
}

// pushSource adds an input source to the tracking set
func (a *autoClose) pushSource(eventSourceID pipeline.EventSourceID) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.sourceSet[eventSourceID] = struct{}{}
}

// autoCloseTable tracks event sources with an active autoClose
type autoCloseTable map[pipeline.EventSourceID]*autoClose

// AutoCloser manages the autoCloseTable
type AutoCloser struct {
	autoCloseTable   autoCloseTable
	autoCloseSources map[pipeline.EventSourceID]struct{}
	router           *eventrouter2.Router
	lock             sync.RWMutex
}

func NewAutoCloser(router *eventrouter2.Router) *AutoCloser {
	return &AutoCloser{
		autoCloseTable:   autoCloseTable{},
		autoCloseSources: map[pipeline.EventSourceID]struct{}{},
		router:           router,
		lock:             sync.RWMutex{},
	}
}

func (s *AutoCloser) addAutoCloseSource(source pipeline.EventSourceID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.autoCloseSources[source] = struct{}{}
}

func (s *AutoCloser) deleteAutoClose(eventSourceID pipeline.EventSourceID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.autoCloseTable, eventSourceID)
}

func (s *AutoCloser) addSourceToDestinationAutoClose(src pipeline.EventSourceID, dests ...pipeline.EventSourceID) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, dest := range dests {
		if _, ok := s.autoCloseTable[dest]; !ok {
			s.autoCloseTable[dest] = &autoClose{
				eventSourceID: dest,
				sourceSet:     map[pipeline.EventSourceID]struct{}{src: {}},
				lock:          sync.RWMutex{},
			}
		}
		s.autoCloseTable[dest].sourceSet[src] = struct{}{}
	}
}

func (s *AutoCloser) hasAutoCloseSource(source pipeline.EventSourceID) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.autoCloseSources[source]
	return ok
}

func (s *AutoCloser) allAutoCloses() []*autoClose {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var out []*autoClose
	for _, ac := range s.autoCloseTable {
		out = append(out, ac)
	}
	return out
}

func (s *AutoCloser) AutoCloseDestination(ctx context.Context, src pipeline.EventSourceID, dests ...pipeline.EventSourceID) {
	if !s.hasAutoCloseSource(src) {
		route, ok := s.router.RouteTo(src)
		if ok {
			go func(src pipeline.EventSourceID, conn *eventrouter2.Route) {
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
							route, ok := s.router.RouteTo(dest.eventSourceID)
							if ok {
								// fmt.Printf("%s auto-closed\n", conn.eventSourceID)
								route.CloseInputChannel()
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
			}(src, route)
			s.addAutoCloseSource(src)
		}
	}
	s.addSourceToDestinationAutoClose(src, dests...)
}
