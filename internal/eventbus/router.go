package eventbus

import (
	"context"
	"fmt"
	"sync"
)

// connection tracks the input and output channels for an event source
type connection struct {
	eventSourceID EventSourceID
	// channel for events processed by the event source (input)
	inputChannel chan Event

	// channel for events produced by the event source (output)
	outputChannel chan Event

	// whether the input channel has been closed, can happen when:
	// 1. the event source has closed its output channel (signalling end-of-output/processing)
	// 2. closed by an auto-close because all input sources into this event source have closed their output channels (end-of-input)
	inputChannelClosed bool

	// both input and output channels are closed
	connectionDoneListeners map[chan<- struct{}]struct{}

	lock sync.RWMutex
}

func (c *connection) NotifyOnConnectionDone(doneCh chan<- struct{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connectionDoneListeners[doneCh] = struct{}{}
}

func (c *connection) notifyConnectionDoneListeners(ctx context.Context) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for doneCh, _ := range c.connectionDoneListeners {
		go func(doneCh chan<- struct{}) {
			select {
			case <-ctx.Done():
				return
			case doneCh <- struct{}{}:
				return
			}
		}(doneCh)
	}
}

func (c *connection) CloseInputChannel() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.inputChannelClosed && c.inputChannel != nil {
		close(c.inputChannel)
		c.inputChannelClosed = true
	}
}

func (c *connection) ConnectionDone(ctx context.Context) {
	c.CloseInputChannel()
	c.notifyConnectionDoneListeners(ctx)
}

type router struct {
	routeTable   map[EventSourceID]*connection
	lock         sync.RWMutex
	debugChannel chan<- Event
}

func (r *router) AddRoute(ctx context.Context, eventSource EventSource) *connection {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.routeTable[eventSource.EventSourceID()]; !ok {
		conn := connection{
			eventSourceID:           eventSource.EventSourceID(),
			inputChannel:            make(chan Event, eventSource.IngressCapacity()),
			outputChannel:           make(chan Event),
			connectionDoneListeners: map[chan<- struct{}]struct{}{},
			inputChannelClosed:      false,
		}
		r.routeTable[eventSource.EventSourceID()] = &conn
		go func(conn *connection) {
			// fmt.Printf("router: starting watch for %s\n", conn.eventSourceID)
			defer func() {
				// fmt.Printf("router: closing watch for %s\n", conn.eventSourceID)
				// once the event source closes its output channel it is no longer taking input
				// delete the route
				r.DeleteRoute(conn.eventSourceID)

				// close the event source input channel
				// this _should_ signal that it should stop processing
				// conn.CloseInputChannel()

				// with both input and output channels closed, signal end
				conn.ConnectionDone(ctx)
			}()
			for {
				// consume output events and route them to their destination
				// while the context is still active and the output channel still contains events or is open
				select {
				case <-ctx.Done():
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
					r.route(ctx, event)
				}
			}
		}(&conn)
	} else {
		fmt.Printf("route: for %s already exists!!!\n", eventSource.EventSourceID())
	}
	return r.routeTable[eventSource.EventSourceID()]
}

func (r *router) DeleteRoute(eventSourceID EventSourceID) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.routeTable, eventSourceID)
}

func (r *router) debug(ctx context.Context, event Event) {
	if r.debugChannel != nil {
		func() {
			select {
			case r.debugChannel <- event:
				return
			case <-ctx.Done():
				return
			}
		}()
	}
}

func (r *router) route(ctx context.Context, event Event) {
	// debug is a blocking call if the debug channel is set
	r.debug(ctx, event)
	if event.Header().IsBroadcastEvent() {
		conns := r.getAllRoutes()
		for _, conn := range conns {
			if conn.eventSourceID != event.Header().Sender() {
				func() {
					select {
					case conn.inputChannel <- event:
						return
					case <-ctx.Done():
						return
					}
				}()
			}
		}
	} else if event.Header().Receiver() != "" {
		conn, ok := r.getRouteTo(event.Header().Receiver())
		if ok {
			select {
			case conn.inputChannel <- event:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *router) getRouteTo(eventSourceID EventSourceID) (*connection, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	conn, ok := r.routeTable[eventSourceID]
	return conn, ok
}

func (r *router) getAllRoutes() []*connection {
	r.lock.RLock()
	defer r.lock.RUnlock()
	var out []*connection
	for _, conn := range r.routeTable {
		out = append(out, conn)
	}
	return out
}
