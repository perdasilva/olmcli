package eventbus

import (
	"context"
	"fmt"
	"sync"
)

type EventBus interface {
	Connect(eventSource EventSource) (<-chan Event, chan<- Event)
}

var _ EventBus = &RouterEventBus{}

type connection struct {
	eventSourceID EventSourceID
	inputChannel  chan Event
	outputChannel chan Event
}

type routeTable map[EventSourceID]connection

type RouterEventBus struct {
	routeTable   routeTable
	lock         sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	debugChannel chan<- Event
}

type RouterEventBusOption func(bus *RouterEventBus)

func WithDebugChannel(debugChannel chan<- Event) RouterEventBusOption {
	return func(bus *RouterEventBus) {
		bus.debugChannel = debugChannel
	}
}

func NewEventBus(parentCtx context.Context, options ...RouterEventBusOption) *RouterEventBus {
	ctx, cancel := context.WithCancel(parentCtx)
	bus := &RouterEventBus{
		routeTable: routeTable{},
		lock:       sync.RWMutex{},
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, applyOption := range options {
		applyOption(bus)
	}
	return bus
}

func (s *RouterEventBus) Connect(eventSource EventSource) (<-chan Event, chan<- Event) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.routeTable[eventSource.EventSourceID()]; !ok {
		conn := connection{
			eventSourceID: eventSource.EventSourceID(),
			inputChannel:  make(chan Event, eventSource.IngressCapacity()),
			outputChannel: make(chan Event),
		}
		s.routeTable[eventSource.EventSourceID()] = conn
		go func(conn *connection) {
			defer func() {
				s.lock.Lock()
				defer s.lock.RLock()
				// close the event source input channel
				// this _should_ signal that it should stop processing
				delete(s.routeTable, conn.eventSourceID)
				close(conn.inputChannel)
			}()
			for {
				select {
				case <-s.ctx.Done():
					// if the context is interrupted
					// don't close the output channel
					// the source is in charge of closing its output channel
					if s.debugChannel != nil {
						close(s.debugChannel)
					}
					return
				case event, hasNext := <-conn.outputChannel:
					// if the output channel is closed
					// this means the event source is done processing
					if !hasNext {
						return
					}
					// fmt.Printf("%s: routing event: %s\n", conn.eventSourceID, event)
					s.route(event)
				}
			}
		}(&conn)
	}
	return s.routeTable[eventSource.EventSourceID()].inputChannel, s.routeTable[eventSource.EventSourceID()].outputChannel
}

func (s *RouterEventBus) Stop() {
	fmt.Println("event bus bailing")
	s.cancel()
}

func (s *RouterEventBus) route(event Event) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.debugChannel != nil {
		func() {
			for {
				select {
				case s.debugChannel <- event:
					return
				case <-s.ctx.Done():
					return
				}
			}
		}()
	}
	if event.Header().IsBroadcastEvent() {
		for _, conn := range s.routeTable {
			if conn.eventSourceID != event.Header().Sender() {
				func() {
					for {
						select {
						case conn.inputChannel <- event:
							return
						case <-s.ctx.Done():
							return
						}
					}
				}()
			}
		}
	} else if event.Header().Receiver() != "" {
		conn, ok := s.routeTable[event.Header().Receiver()]
		if ok {
			for {
				select {
				case conn.inputChannel <- event:
					return
				case <-s.ctx.Done():
					return
				}
			}
		}
	}
}
