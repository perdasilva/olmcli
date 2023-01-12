package eventsource

//type EventProducer[I interface{}] struct {
//	eventSourceID   pipeline.EventSourceID
//	ingressCapacity int
//	eventFactory    pipeline.EventFactory[I]
//	inputChannel    <-chan pipeline.Event
//	outputChannel   chan<- pipeline.Event
//	producer        pipeline.Producer[I]
//	done            chan struct{}
//	reason          pipeline.ErrorEvent
//	lock            sync.RWMutex
//	state           pipeline.EventSourceState
//	outputs         pipeline.EventSourceSet
//	bus             pipeline.EventBus
//}
//
//func NewEventProducer[I interface{}](id pipeline.EventSourceID, producer pipeline.Producer[I], bus pipeline.EventBus) *EventProducer[I] {
//	source := &EventProducer[I]{
//		eventSourceID:   id,
//		ingressCapacity: 0,
//		producer:        producer,
//		eventFactory:    event.NewEventFactory[I](id),
//		done:            make(chan struct{}),
//		lock:            sync.RWMutex{},
//		state:           pipeline.EventSourceStateInactive,
//		outputs:         pipeline.EventSourceSet{},
//		bus:             bus,
//	}
//	source.inputChannel, source.outputChannel = bus.Connect(source)
//	return source
//}
//
//func (e *EventProducer[I]) Done() <-chan struct{} {
//	return e.done
//}
//
//func (e *EventProducer[I]) AddOutput(outputSourceIds ...pipeline.EventSourceID) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//	for _, id := range outputSourceIds {
//		if _, ok := e.outputs[id]; !ok {
//			e.outputs[id] = struct{}{}
//			e.bus.AutoClosDestination(e.eventSourceID, id)
//		}
//	}
//}
//
//func (e *EventProducer[I]) Reason() pipeline.ErrorEvent {
//	e.lock.RLock()
//	defer e.lock.RUnlock()
//	return e.reason
//}
//
//func (e *EventProducer[I]) State() pipeline.EventSourceState {
//	e.lock.RLock()
//	defer e.lock.RUnlock()
//	return e.state
//}
//
//func (e *EventProducer[I]) setReason(errEvt pipeline.ErrorEvent) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//	e.reason = errEvt
//}
//
//func (e *EventProducer[I]) setState(state pipeline.EventSourceState) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//	e.state = state
//}
//
//func (e *EventProducer[I]) Start(ctx context.Context) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//
//	if e.state != pipeline.EventSourceStateInactive {
//		return
//	}
//	e.state = pipeline.EventSourceStateActive
//	//fmt.Printf("%s: starting\n", e.eventSourceID)
//	go func() {
//		defer func() {
//			close(e.outputChannel)
//			// fmt.Printf("%s: closing output channel\n", e.eventSourceID)
//			e.done <- struct{}{}
//		}()
//		for {
//			select {
//			case <-ctx.Done():
//				e.setState(pipeline.EventSourceStateInterrupted)
//				return
//			case event, hasNext := <-e.inputChannel:
//				// fmt.Printf("%s: got event: %s\n", e.eventSourceID, event.Header().EventID())
//				if !hasNext {
//					return
//				}
//				switch evt := event.(type) {
//				case pipeline.ErrorEvent:
//					e.setReason(evt)
//					e.setState(pipeline.EventSourceStateAborted)
//					return
//				}
//			default:
//				data, err := e.producer.Produce()
//				if err != nil {
//					// fmt.Printf("%s: producer err: %s\n", e.eventSourceID, err)
//					errEvent := e.eventFactory.NewErrorEvent(err)
//					errEvent.Broadcast()
//					sendEvent(ctx, e.outputChannel, errEvent)
//					e.setReason(errEvent)
//					e.setState(pipeline.EventSourceStateFailed)
//					return
//				}
//				if data == nil {
//					// fmt.Printf("%s: producer finished\n", e.eventSourceID)
//					e.setState(pipeline.EventSourceStateSuccess)
//					return
//				}
//				func() {
//					e.lock.RLock()
//					defer e.lock.RUnlock()
//					dataEvent := e.eventFactory.NewDataEvent(*data)
//					outputIds := e.outputs.EventSourceIDs()
//					for i := 0; i < len(outputIds); i++ {
//						evt := dataEvent.Copy()
//						evt.Route(outputIds[i])
//						//fmt.Printf("%s: producer emitting event to %s\n", e.eventSourceID, outputIds[i])
//						sendEvent(ctx, e.outputChannel, evt)
//					}
//				}()
//			}
//		}
//	}()
//}
//
//func (e *EventProducer[I]) IngressCapacity() int {
//	return e.ingressCapacity
//}
//
//func (e *EventProducer[I]) EventSourceID() pipeline.EventSourceID {
//	return e.eventSourceID
//}
