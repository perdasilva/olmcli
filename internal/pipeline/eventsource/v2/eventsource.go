package v2

import (
	"context"
	"sync"

	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/pipeline/event"
)

type EventSource[D interface{}, P pipeline.ProcessContext] struct {
	eventSourceID   pipeline.EventSourceID
	ingressCapacity int
	eventFactory    pipeline.EventFactory[D]
	inputChannel    <-chan pipeline.Event
	outputChannel   chan<- pipeline.Event
	process         pipeline.Process[P]
	done            chan struct{}
	reason          pipeline.ErrorEvent
	lock            sync.RWMutex
	state           pipeline.EventSourceState
	outputs         pipeline.EventSourceSet
	bus             pipeline.EventBus
}

func newEventSource[D interface{}, P pipeline.ProcessContext](id pipeline.EventSourceID, process pipeline.Process[P], bus pipeline.EventBus) *EventSource[D, P] {
	source := &EventSource[D, P]{
		eventSourceID:   id,
		ingressCapacity: 0,
		process:         process,
		eventFactory:    event.NewEventFactory[D](id),
		done:            make(chan struct{}),
		lock:            sync.RWMutex{},
		state:           pipeline.EventSourceStateInactive,
		outputs:         pipeline.EventSourceSet{},
		bus:             bus,
	}
	source.inputChannel, source.outputChannel = bus.Connect(source)
	return source
}

func NewProducerEventSource[D interface{}](id pipeline.EventSourceID, process pipeline.Producer[D], bus pipeline.EventBus) pipeline.ProducerEventSource {
	return newEventSource[D, pipeline.ProducerContext[D]](id, process, bus)
}

func NewConsumerEventSource[D interface{}](id pipeline.EventSourceID, process pipeline.Consumer[D], bus pipeline.EventBus) pipeline.ConsumerEventSource {
	return newEventSource[D, pipeline.ConsumerContext[D]](id, process, bus)
}

func NewProcessorEventSource[D interface{}](id pipeline.EventSourceID, process pipeline.Processor[D], bus pipeline.EventBus) pipeline.ProcessorEventSource {
	return newEventSource[D, pipeline.ProcessorContext[D]](id, process, bus)
}

func (e *EventSource[D, P]) Done() <-chan struct{} {
	return e.done
}

func (e *EventSource[D, P]) Reason() pipeline.ErrorEvent {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.reason
}
func (e *EventSource[D, P]) State() pipeline.EventSourceState {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.state
}

func (e *EventSource[D, P]) AddOutput(outputSourceIds ...pipeline.EventSourceID) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, id := range outputSourceIds {
		if _, ok := e.outputs[id]; !ok {
			e.outputs[id] = struct{}{}
			e.bus.AutoClosDestination(e.eventSourceID, id)
		}
	}
}

func (e *EventSource[D, P]) setReason(errEvt pipeline.ErrorEvent) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.reason = errEvt
}

func (e *EventSource[D, P]) setState(state pipeline.EventSourceState) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.state = state
}

func (e *EventSource[D, P]) Start(ctx context.Context) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.state != pipeline.EventSourceStateInactive {
		return
	}
	e.state = pipeline.EventSourceStateActive

	pCtx := newProcessContext[D](ctx, e.ingressCapacity)
	// start executing event source process
	go func() {
		var c pipeline.ProcessorContext[D] = pCtx
		e.process.Execute(c.(P))
	}()

	// read events coming from the bus and pipe them to the process
	go func() {
		defer func() {
			pCtx.closeInputChannel()
		}()
		for {
			select {
			case <-pCtx.done:
				return
			// if the context is done, bail
			case <-ctx.Done():
				e.setState(pipeline.EventSourceStateInterrupted)
				return
			// pipe data from data events into the process
			// error events should cause the event source to abort
			case evt, hasNext := <-e.inputChannel:
				if !hasNext {
					return
				}
				switch v := evt.(type) {
				case pipeline.DataEvent[D]:
					pCtx.sendInput(v.Data())
				case pipeline.ErrorEvent:
					e.setReason(v)
					e.setState(pipeline.EventSourceStateAborted)
					return
				}
			}
		}
	}()

	// read output from the process and pipe it to the bus
	go func() {
		defer func() {
			<-pCtx.done
			if e.State() == pipeline.EventSourceStateActive {
				if pCtx.Error() != nil {
					errEvent := e.eventFactory.NewErrorEvent(pCtx.err)
					errEvent.Broadcast()
					sendEvent(ctx, e.outputChannel, errEvent)
					e.setReason(errEvent)
					e.setState(pipeline.EventSourceStateFailed)
				} else {
					e.setState(pipeline.EventSourceStateSuccess)
				}
			}
			pCtx.closeInputChannel()
			close(e.outputChannel)
			close(e.done)
		}()
		for {
			select {
			// if the context is done, bail
			case <-ctx.Done():
				e.setState(pipeline.EventSourceStateInterrupted)
				return
			// pipe data produced by the process to the event sources connected to this one
			case data, hasNext := <-pCtx.outputChannel:
				if !hasNext {
					return
				}
				dataEvent := e.eventFactory.NewDataEvent(data)
				e.sendEventToOutputs(ctx, dataEvent)
			}
		}
	}()
}

func (e *EventSource[D, P]) IngressCapacity() int {
	return e.ingressCapacity
}

func (e *EventSource[D, P]) EventSourceID() pipeline.EventSourceID {
	return e.eventSourceID
}

func (e *EventSource[D, P]) sendEventToOutputs(ctx context.Context, event pipeline.DataEvent[D]) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	outputIds := e.outputs.EventSourceIDs()
	for i := 0; i < len(outputIds); i++ {
		evt := event.Copy()
		evt.Route(outputIds[i])
		sendEvent(ctx, e.outputChannel, evt)
	}
}

func sendEvent(ctx context.Context, outputChannel chan<- pipeline.Event, event pipeline.Event) {
	select {
	case <-ctx.Done():
		return
	case outputChannel <- event:
		return
	}
}

type processContext[D interface{}] struct {
	inputChannel         chan D
	outputChannel        chan D
	done                 chan struct{}
	ctx                  context.Context
	isInputChannelClosed bool
	lock                 sync.RWMutex
	err                  error
}

var _ pipeline.ProcessContext = &processContext[interface{}]{}
var _ pipeline.ProcessorContext[interface{}] = &processContext[interface{}]{}
var _ pipeline.ConsumerContext[interface{}] = &processContext[interface{}]{}
var _ pipeline.ProducerContext[interface{}] = &processContext[interface{}]{}

func newProcessContext[D interface{}](ctx context.Context, ingressCapacity int) *processContext[D] {
	return &processContext[D]{
		ctx:                  ctx,
		inputChannel:         make(chan D, ingressCapacity),
		outputChannel:        make(chan D),
		done:                 make(chan struct{}),
		isInputChannelClosed: false,
		lock:                 sync.RWMutex{},
	}
}

func (c *processContext[D]) Read() (D, bool) {
	select {
	case <-c.ctx.Done():
		return *new(D), false
	case data, hasNext := <-c.inputChannel:
		return data, hasNext
	}
}

func (c *processContext[D]) Write(data D) {
	select {
	case <-c.ctx.Done():
		return
	case c.outputChannel <- data:
		return
	}
}

func (c *processContext[D]) Close() {
	// not protecting closing with lock and bool
	// so process authors can get panics when attempting to close a closed channel
	// should signal a bug in their code
	close(c.outputChannel)
	close(c.done)
}

func (c *processContext[D]) CloseWithError(err error) {
	func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		c.err = err
	}()
	c.Close()
}

func (c *processContext[D]) Error() error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.err
}

func (c *processContext[D]) closeInputChannel() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.isInputChannelClosed {
		close(c.inputChannel)
	}
	c.isInputChannelClosed = true
}

func (c *processContext[D]) sendInput(data D) {
	select {
	case <-c.done:
	case <-c.ctx.Done():
	case c.inputChannel <- data:
	}
}
