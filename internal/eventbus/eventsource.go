package eventbus

import (
	"context"
	"sync"
)

type EventSourceID string
type EventSourceState string

type OutputEventSourceSet map[EventSourceID]struct{}

func (o OutputEventSourceSet) EventSourceIDs() []EventSourceID {
	ids := make([]EventSourceID, 0, len(o))
	for id, _ := range o {
		ids = append(ids, id)
	}
	return ids
}

const (
	EventSourceStateInactive    EventSourceState = "INACTIVE"
	EventSourceStateActive      EventSourceState = "ACTIVE"
	EventSourceStateSuccess     EventSourceState = "SUCCESS"
	EventSourceStateFailed      EventSourceState = "FAILED"
	EventSourceStateAborted     EventSourceState = "ABORTED"
	EventSourceStateInterrupted EventSourceState = "INTERRUPTED"
)

type EventSource interface {
	IngressCapacity() int
	EventSourceID() EventSourceID
}

type Producer[I interface{}] interface {
	Produce() (*I, error)
}

type Consumer[I interface{}] interface {
	Consume(data I) error
}

type Processor[I interface{}] interface {
	InputFinished() (<-chan I, <-chan error)
	Process(data I) (*I, error)
}

type EventProducer[I interface{}] struct {
	eventSourceID   EventSourceID
	ingressCapacity int
	eventFactory    EventFactory[I]
	inputChannel    <-chan Event
	outputChannel   chan<- Event
	producer        Producer[I]
	done            chan struct{}
	reason          ErrorEvent
	lock            sync.RWMutex
	state           EventSourceState
	outputs         OutputEventSourceSet
}

func NewEventProducer[I interface{}](id EventSourceID, producer Producer[I], bus EventBus) *EventProducer[I] {
	source := &EventProducer[I]{
		eventSourceID:   id,
		ingressCapacity: 0,
		producer:        producer,
		eventFactory:    NewEventFactory[I](id),
		done:            make(chan struct{}),
		lock:            sync.RWMutex{},
		state:           EventSourceStateInactive,
		outputs:         OutputEventSourceSet{},
	}
	source.inputChannel, source.outputChannel = bus.Connect(source)
	return source
}

func (e *EventProducer[I]) Done() <-chan struct{} {
	return e.done
}

func (e *EventProducer[I]) AddOutput(outputSources ...EventSourceID) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, id := range outputSources {
		if _, ok := e.outputs[id]; !ok {
			e.outputs[id] = struct{}{}
		}
	}
}

func (e *EventProducer[I]) Reason() ErrorEvent {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.reason
}

func (e *EventProducer[I]) State() EventSourceState {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.state
}

func (e *EventProducer[I]) setReason(errEvt ErrorEvent) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.reason = errEvt
}

func (e *EventProducer[I]) setState(state EventSourceState) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.state = state
}

func (e *EventProducer[I]) Start(ctx context.Context) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.state != EventSourceStateInactive {
		return
	}
	e.state = EventSourceStateActive

	go func() {
		defer func() {
			close(e.outputChannel)
			e.done <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				e.setState(EventSourceStateInterrupted)
				return
			case event, hasNext := <-e.inputChannel:
				if !hasNext {
					return
				}
				switch evt := event.(type) {
				case ErrorEvent:
					e.setReason(evt)
					e.setState(EventSourceStateAborted)
					return
				}
			default:
				data, err := e.producer.Produce()
				if err != nil {
					errEvent := e.eventFactory.NewErrorEvent(err)
					errEvent.Broadcast()
					sendEvent(ctx, e.outputChannel, errEvent)
					e.setReason(errEvent)
					e.setState(EventSourceStateFailed)
					return
				}
				if data == nil {
					e.setState(EventSourceStateSuccess)
					return
				}
				func() {
					e.lock.RLock()
					defer e.lock.RUnlock()
					dataEvent := e.eventFactory.NewDataEvent(*data)
					outputIds := e.outputs.EventSourceIDs()
					for i := 0; i < len(outputIds); i++ {
						select {
						case <-ctx.Done():
							return
						default:
							evt := dataEvent.Copy()
							evt.Route(outputIds[i])
							sendEvent(ctx, e.outputChannel, evt)
						}
					}
				}()
			}
		}
	}()
}

func (e *EventProducer[I]) IngressCapacity() int {
	return e.ingressCapacity
}

func (e *EventProducer[I]) EventSourceID() EventSourceID {
	return e.eventSourceID
}

type EventConsumer[I interface{}] struct {
	eventSourceID   EventSourceID
	ingressCapacity int
	eventFactory    EventFactory[I]
	inputChannel    <-chan Event
	outputChannel   chan<- Event
	consumer        Consumer[I]
	done            chan struct{}
	reason          ErrorEvent
	lock            sync.RWMutex
	state           EventSourceState
}

func NewEventConsumer[I interface{}](id EventSourceID, consumer Consumer[I], bus EventBus) *EventConsumer[I] {
	source := &EventConsumer[I]{
		eventSourceID:   id,
		ingressCapacity: 0,
		consumer:        consumer,
		eventFactory:    NewEventFactory[I](id),
		done:            make(chan struct{}),
		lock:            sync.RWMutex{},
		state:           EventSourceStateInactive,
	}
	source.inputChannel, source.outputChannel = bus.Connect(source)
	return source
}

func (e *EventConsumer[I]) Done() <-chan struct{} {
	return e.done
}

func (e *EventConsumer[I]) Reason() ErrorEvent {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.reason
}

func (e *EventConsumer[I]) State() EventSourceState {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.state
}

func (e *EventConsumer[I]) setReason(errEvt ErrorEvent) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.reason = errEvt
}

func (e *EventConsumer[I]) setState(state EventSourceState) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.state = state
}

func (e *EventConsumer[I]) Start(ctx context.Context) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.state != EventSourceStateInactive {
		return
	}
	e.state = EventSourceStateActive

	go func() {
		defer func() {
			close(e.outputChannel)
			e.done <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				e.setState(EventSourceStateInterrupted)
				return
			case event, hasNext := <-e.inputChannel:
				if !hasNext {
					e.setState(EventSourceStateSuccess)
					return
				}
				switch evt := event.(type) {
				case ErrorEvent:
					e.setReason(evt)
					e.setState(EventSourceStateAborted)
					return
				case DataEvent[I]:
					err := e.consumer.Consume(evt.Data())
					if err != nil {
						errEvent := e.eventFactory.NewErrorEvent(err)
						sendEvent(ctx, e.outputChannel, errEvent)
						e.setReason(errEvent)
						e.setState(EventSourceStateFailed)
						return
					}
				}
			}
		}
	}()
}

func (e *EventConsumer[I]) IngressCapacity() int {
	return e.ingressCapacity
}

func (e *EventConsumer[I]) EventSourceID() EventSourceID {
	return e.eventSourceID
}

type EventProcessor[I interface{}] struct {
	eventSourceID   EventSourceID
	ingressCapacity int
	eventFactory    EventFactory[I]
	inputChannel    <-chan Event
	outputChannel   chan<- Event
	processor       Processor[I]
	done            chan struct{}
	reason          ErrorEvent
	lock            sync.RWMutex
	state           EventSourceState
	outputs         OutputEventSourceSet
}

func NewEventProcessor[I interface{}](id EventSourceID, processor Processor[I], bus EventBus) *EventProcessor[I] {
	source := &EventProcessor[I]{
		eventSourceID:   id,
		ingressCapacity: 0,
		processor:       processor,
		eventFactory:    NewEventFactory[I](id),
		done:            make(chan struct{}),
		lock:            sync.RWMutex{},
		state:           EventSourceStateInactive,
		outputs:         OutputEventSourceSet{},
	}
	source.inputChannel, source.outputChannel = bus.Connect(source)
	return source
}

func (e *EventProcessor[I]) Done() <-chan struct{} {
	return e.done
}

func (e *EventProcessor[I]) Reason() ErrorEvent {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.reason
}
func (e *EventProcessor[I]) State() EventSourceState {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.state
}

func (e *EventProcessor[I]) AddOutput(outputSources ...EventSourceID) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, id := range outputSources {
		if _, ok := e.outputs[id]; !ok {
			e.outputs[id] = struct{}{}
		}
	}
}

func (e *EventProcessor[I]) setReason(errEvt ErrorEvent) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.reason = errEvt
}

func (e *EventProcessor[I]) setState(state EventSourceState) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.state = state
}

func (e *EventProcessor[I]) Start(ctx context.Context) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.state != EventSourceStateInactive {
		return
	}
	e.state = EventSourceStateActive

	go func() {
		defer func() {
			close(e.outputChannel)
			e.done <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				e.setState(EventSourceStateInterrupted)
				return
			case event, hasNext := <-e.inputChannel:
				if !hasNext {
					dataChannel, errChannel := e.processor.InputFinished()
					if dataChannel == nil && errChannel == nil {
						e.setState(EventSourceStateSuccess)
						return
					}
					for {
						select {
						case err := <-errChannel:
							errEvent := e.eventFactory.NewErrorEvent(err)
							sendEvent(ctx, e.outputChannel, errEvent)
							e.setReason(errEvent)
							e.setState(EventSourceStateFailed)
							return
						case data, hasNext := <-dataChannel:
							if !hasNext {
								e.setState(EventSourceStateSuccess)
								return
							}
							func() {
								e.lock.RLock()
								defer e.lock.RUnlock()
								dataEvent := e.eventFactory.NewDataEvent(data)
								outputIds := e.outputs.EventSourceIDs()
								for i := 0; i < len(outputIds); i++ {
									select {
									case <-ctx.Done():
										return
									default:
										evt := dataEvent.Copy()
										evt.Route(outputIds[i])
										sendEvent(ctx, e.outputChannel, evt)
									}
								}
							}()
						}
					}
				}
				switch evt := event.(type) {
				case ErrorEvent:
					e.setReason(evt)
					e.setState(EventSourceStateAborted)
					return
				case DataEvent[I]:
					data, err := e.processor.Process(evt.Data())
					if err != nil {
						errEvent := e.eventFactory.NewErrorEvent(err)
						sendEvent(ctx, e.outputChannel, errEvent)
						e.setReason(errEvent)
						e.setState(EventSourceStateFailed)
						return
					}
					if data != nil {
						func() {
							e.lock.RLock()
							defer e.lock.RUnlock()
							dataEvent := e.eventFactory.NewDataEvent(*data)
							outputIds := e.outputs.EventSourceIDs()
							for i := 0; i < len(outputIds); i++ {
								select {
								case <-ctx.Done():
									return
								default:
									evt := dataEvent.Copy()
									evt.Route(outputIds[i])
									sendEvent(ctx, e.outputChannel, evt)
								}
							}
						}()
					}
				}
			}
		}
	}()
}

func (e *EventProcessor[I]) IngressCapacity() int {
	return e.ingressCapacity
}

func (e *EventProcessor[I]) EventSourceID() EventSourceID {
	return e.eventSourceID
}

func sendEvent(ctx context.Context, outputChannel chan<- Event, event Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case outputChannel <- event:
			return
		}
	}
}
