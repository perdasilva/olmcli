package eventsource

//var _ pipeline.EventSource = &EventConsumer[interface{}]{}
//
//type EventConsumer[I interface{}] struct {
//	eventSourceID   pipeline.EventSourceID
//	ingressCapacity int
//	eventFactory    pipeline.EventFactory[I]
//	inputChannel    <-chan pipeline.Event
//	outputChannel   chan<- pipeline.Event
//	consumer        pipeline.Consumer[I]
//	done            chan struct{}
//	reason          pipeline.ErrorEvent
//	lock            sync.RWMutex
//	state           pipeline.EventSourceState
//}
//
//func NewEventConsumer[I interface{}](id pipeline.EventSourceID, consumer pipeline.Consumer[I], bus pipeline.EventBus) *EventConsumer[I] {
//	source := &EventConsumer[I]{
//		eventSourceID:   id,
//		ingressCapacity: 0,
//		consumer:        consumer,
//		eventFactory:    event.NewEventFactory[I](id),
//		done:            make(chan struct{}),
//		lock:            sync.RWMutex{},
//		state:           pipeline.EventSourceStateInactive,
//	}
//	source.inputChannel, source.outputChannel = bus.Connect(source)
//	return source
//}
//
//func (e *EventConsumer[I]) Done() <-chan struct{} {
//	return e.done
//}
//
//func (e *EventConsumer[I]) Reason() pipeline.ErrorEvent {
//	e.lock.RLock()
//	defer e.lock.RUnlock()
//	return e.reason
//}
//
//func (e *EventConsumer[I]) State() pipeline.EventSourceState {
//	e.lock.RLock()
//	defer e.lock.RUnlock()
//	return e.state
//}
//
//func (e *EventConsumer[I]) setReason(errEvt pipeline.ErrorEvent) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//	e.reason = errEvt
//}
//
//func (e *EventConsumer[I]) setState(state pipeline.EventSourceState) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//	e.state = state
//}
//
//func (e *EventConsumer[I]) Start(ctx context.Context) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//
//	if e.state != pipeline.EventSourceStateInactive {
//		return
//	}
//	e.state = pipeline.EventSourceStateActive
//
//	go func() {
//		defer func() {
//			close(e.outputChannel)
//			e.done <- struct{}{}
//		}()
//		for {
//			select {
//			case <-ctx.Done():
//				e.setState(pipeline.EventSourceStateInterrupted)
//				return
//			case event, hasNext := <-e.inputChannel:
//				if !hasNext {
//					e.setState(pipeline.EventSourceStateSuccess)
//					return
//				}
//				event.Visit(e.eventSourceID)
//				switch evt := event.(type) {
//				case pipeline.ErrorEvent:
//					e.setReason(evt)
//					e.setState(pipeline.EventSourceStateAborted)
//					return
//				case pipeline.DataEvent[I]:
//					err := e.consumer.Consume(evt.Data())
//					if err != nil {
//						errEvent := e.eventFactory.NewErrorEvent(err)
//						sendEvent(ctx, e.outputChannel, errEvent)
//						e.setReason(errEvent)
//						e.setState(pipeline.EventSourceStateFailed)
//						return
//					}
//				}
//			}
//		}
//	}()
//}
//
//func (e *EventConsumer[I]) IngressCapacity() int {
//	return e.ingressCapacity
//}
//
//func (e *EventConsumer[I]) EventSourceID() pipeline.EventSourceID {
//	return e.eventSourceID
//}
