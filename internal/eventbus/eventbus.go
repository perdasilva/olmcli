package eventbus

import (
	"context"
	"sync"
)

type EventBus interface {
	Connect(eventSource EventSource) (<-chan Event, chan<- Event)
	AutoClosDestination(src EventSourceID, dests ...EventSourceID)
}

var _ EventBus = &RouterEventBus{}

type RouterEventBus struct {
	router     *router
	autoCloser *autoCloser
	ctx        context.Context
	cancel     context.CancelFunc
}

type RouterEventBusOption func(bus *RouterEventBus)

func WithDebugChannel(debugChannel chan<- Event) RouterEventBusOption {
	return func(bus *RouterEventBus) {
		bus.router.debugChannel = debugChannel
	}
}

func NewEventBus(parentCtx context.Context, options ...RouterEventBusOption) *RouterEventBus {
	ctx, cancel := context.WithCancel(parentCtx)
	router := &router{
		lock:       sync.RWMutex{},
		routeTable: map[EventSourceID]*connection{},
	}
	bus := &RouterEventBus{
		router:     router,
		autoCloser: newAutoCloser(router),
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, applyOption := range options {
		applyOption(bus)
	}

	return bus
}

func (s *RouterEventBus) Connect(eventSource EventSource) (<-chan Event, chan<- Event) {
	conn := s.router.AddRoute(s.ctx, eventSource)
	return conn.inputChannel, conn.outputChannel
}

func (s *RouterEventBus) AutoClosDestination(src EventSourceID, dests ...EventSourceID) {
	s.autoCloser.AutoCloseDestination(s.ctx, src, dests...)
}

func (s *RouterEventBus) Stop() {
	s.cancel()
}
