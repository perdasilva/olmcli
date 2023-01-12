package eventbus

import (
	"context"

	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/pipeline/eventbus/autocloser"
	"github.com/perdasilva/olmcli/internal/pipeline/eventbus/eventrouter"
)

var _ pipeline.EventBus = &RouterEventBus{}

type RouterEventBus struct {
	router     *eventrouter.Router
	autoCloser *autocloser.AutoCloser
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewEventBus(parentCtx context.Context, options ...eventrouter.Option) *RouterEventBus {
	ctx, cancel := context.WithCancel(parentCtx)
	router := eventrouter.NewEventRouter(ctx, options...)
	bus := &RouterEventBus{
		router:     router,
		autoCloser: autocloser.NewAutoCloser(router),
		ctx:        ctx,
		cancel:     cancel,
	}
	return bus
}

func (s *RouterEventBus) Connect(eventSource pipeline.EventSource) (<-chan pipeline.Event, chan<- pipeline.Event) {
	route := s.router.AddRoute(eventSource)
	return route.InputChannel(), route.OutputChannel()
}

func (s *RouterEventBus) AutoClosDestination(src pipeline.EventSourceID, dests ...pipeline.EventSourceID) {
	s.autoCloser.AutoCloseDestination(s.ctx, src, dests...)
}

func (s *RouterEventBus) Stop() {
	s.router.Close()
	s.cancel()
}
