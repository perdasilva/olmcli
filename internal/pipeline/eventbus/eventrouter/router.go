package eventrouter

import (
	"context"
	"fmt"
	"sync"

	"github.com/perdasilva/olmcli/internal/pipeline"
)

type Router struct {
	routeTable   map[pipeline.EventSourceID]*Route
	lock         sync.RWMutex
	debugChannel chan<- pipeline.Event
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewEventRouter(ctx context.Context, opts ...Option) *Router {
	routerContext, cancel := context.WithCancel(ctx)
	router := &Router{
		routeTable: map[pipeline.EventSourceID]*Route{},
		lock:       sync.RWMutex{},
		ctx:        routerContext,
		cancel:     cancel,
	}
	for _, applyOption := range opts {
		applyOption(router)
	}

	// close debug channel when context expires
	if router.debugChannel != nil {
		go func() {
			<-router.ctx.Done()
			close(router.debugChannel)
		}()
	}

	return router
}

func (r *Router) AddRoute(eventSource pipeline.EventSource) *Route {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.routeTable[eventSource.EventSourceID()]; !ok {
		conn := Route{
			eventSourceID:           eventSource.EventSourceID(),
			inputChannel:            make(chan pipeline.Event, eventSource.IngressCapacity()),
			outputChannel:           make(chan pipeline.Event),
			connectionDoneListeners: map[chan<- struct{}]struct{}{},
			inputChannelClosed:      false,
		}
		r.routeTable[eventSource.EventSourceID()] = &conn
		go func(conn *Route) {
			// fmt.Printf("Router: starting watch for %s\n", conn.eventSourceID)
			defer func() {
				// fmt.Printf("Router: closing watch for %s\n", conn.eventSourceID)
				// once the event source closes its output channel it is no longer taking input
				// delete the route
				r.DeleteRoute(conn.eventSourceID)

				// close the event source input channel
				// this _should_ signal that it should stop processing
				// conn.CloseInputChannel()

				// with both input and output channels closed, signal end
				conn.ConnectionDone(r.ctx)
			}()
			for {
				// consume output events and route them to their destination
				// while the context is still active and the output channel still contains events or is open
				select {
				case <-r.ctx.Done():
					// if the context is interrupted
					// don't close the output channel
					// the source is in charge of closing its output channel
					return
				case event, hasNext := <-conn.outputChannel:
					// if the output channel is inputChannelClosed
					// this means the event source is done processing
					if !hasNext {
						return
					}
					r.route(event)
				}
			}
		}(&conn)
	} else {
		fmt.Printf("route: for %s already exists!!!\n", eventSource.EventSourceID())
	}
	return r.routeTable[eventSource.EventSourceID()]
}

func (r *Router) DeleteRoute(eventSourceID pipeline.EventSourceID) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.routeTable, eventSourceID)
}

func (r *Router) Close() {
	r.cancel()
}

func (r *Router) debug(event pipeline.Event) {
	if r.debugChannel != nil {
		select {
		case r.debugChannel <- event:
			return
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Router) route(event pipeline.Event) {
	// debug is a blocking call if the debug channel is set
	r.debug(event)
	if event.Header().IsBroadcastEvent() {
		conns := r.getAllRoutes()
		for _, conn := range conns {
			if conn.eventSourceID != event.Header().Sender() {
				func() {
					select {
					case conn.inputChannel <- event:
						return
					case <-r.ctx.Done():
						return
					}
				}()
			}
		}
	} else if event.Header().Receiver() != "" {
		conn, ok := r.RouteTo(event.Header().Receiver())
		if ok {
			select {
			case conn.inputChannel <- event:
				return
			case <-r.ctx.Done():
				return
			}
		}
	}
}

func (r *Router) RouteTo(eventSourceID pipeline.EventSourceID) (*Route, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	conn, ok := r.routeTable[eventSourceID]
	return conn, ok
}

func (r *Router) getAllRoutes() []*Route {
	r.lock.RLock()
	defer r.lock.RUnlock()
	var out []*Route
	for _, conn := range r.routeTable {
		out = append(out, conn)
	}
	return out
}
