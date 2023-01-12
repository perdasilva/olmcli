package eventsource

//type EventProcessor[I interface{}] struct {
//	eventSourceID   pipeline.EventSourceID
//	ingressCapacity int
//	eventFactory    pipeline.EventFactory[I]
//	inputChannel    <-chan pipeline.Event
//	outputChannel   chan<- pipeline.Event
//	processor       pipeline.Processor[I]
//	done            chan struct{}
//	reason          pipeline.ErrorEvent
//	lock            sync.RWMutex
//	state           pipeline.EventSourceState
//	outputs         pipeline.EventSourceSet
//	bus             pipeline.EventBus
//}
//
//func NewEventProcessor[I interface{}](id pipeline.EventSourceID, processor pipeline.Processor[I], bus pipeline.EventBus) *EventProcessor[I] {
//	source := &EventProcessor[I]{
//		eventSourceID:   id,
//		ingressCapacity: 0,
//		processor:       processor,
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
//func (e *EventProcessor[I]) Done() <-chan struct{} {
//	return e.done
//}
//
//func (e *EventProcessor[I]) Reason() pipeline.ErrorEvent {
//	e.lock.RLock()
//	defer e.lock.RUnlock()
//	return e.reason
//}
//func (e *EventProcessor[I]) State() pipeline.EventSourceState {
//	e.lock.RLock()
//	defer e.lock.RUnlock()
//	return e.state
//}
//
//func (e *EventProcessor[I]) AddOutput(outputSourceIds ...pipeline.EventSourceID) {
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
//func (e *EventProcessor[I]) setReason(errEvt pipeline.ErrorEvent) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//	e.reason = errEvt
//}
//
//func (e *EventProcessor[I]) setState(state pipeline.EventSourceState) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//	e.state = state
//}
//
//func (e *EventProcessor[I]) Start(ctx context.Context) {
//	e.lock.Lock()
//	defer e.lock.Unlock()
//
//	if e.state != pipeline.EventSourceStateInactive {
//		return
//	}
//	e.state = pipeline.EventSourceStateActive
//
//	//fmt.Printf("%s: starting\n", e.eventSourceID)
//
//	go func() {
//		defer func() {
//			close(e.outputChannel)
//			//fmt.Printf("%s: closing output channel\n", e.eventSourceID)
//			e.done <- struct{}{}
//		}()
//		for {
//			select {
//			case <-ctx.Done():
//				e.setState(pipeline.EventSourceStateInterrupted)
//				return
//			case event, hasNext := <-e.inputChannel:
//				if !hasNext {
//					//fmt.Printf("%s: calling input finished hook\n", e.eventSourceID)
//					dataChannel, errChannel := e.processor.InputFinished()
//					if dataChannel == nil && errChannel == nil {
//						//fmt.Printf("%s: nothing to do...successs\n", e.eventSourceID)
//						e.setState(pipeline.EventSourceStateSuccess)
//						return
//					}
//					for {
//						select {
//						case data, hasNext := <-dataChannel:
//							if !hasNext {
//								//fmt.Printf("%s: ran out of data...success\n", e.eventSourceID)
//								e.setState(pipeline.EventSourceStateSuccess)
//								return
//							}
//							func() {
//								e.lock.RLock()
//								defer e.lock.RUnlock()
//								dataEvent := e.eventFactory.NewDataEvent(data)
//								outputIds := e.outputs.EventSourceIDs()
//								for i := 0; i < len(outputIds); i++ {
//									evt := dataEvent.Copy()
//									evt.Route(outputIds[i])
//									//fmt.Printf("%s: emitting event to %s\n", e.eventSourceID, outputIds[i])
//									sendEvent(ctx, e.outputChannel, evt)
//								}
//							}()
//						case err, ok := <-errChannel:
//							if ok {
//								//fmt.Printf("%s: got error %s...failing\n", e.eventSourceID, err)
//								errEvent := e.eventFactory.NewErrorEvent(err)
//								errEvent.Broadcast()
//								sendEvent(ctx, e.outputChannel, errEvent)
//								e.setReason(errEvent)
//								e.setState(pipeline.EventSourceStateFailed)
//							} else {
//								//fmt.Printf("%s: no errors...success\n", e.eventSourceID)
//								e.setState(pipeline.EventSourceStateSuccess)
//							}
//							return
//						}
//					}
//				}
//				event.Visit(e.eventSourceID)
//				switch evt := event.(type) {
//				case pipeline.ErrorEvent:
//					//fmt.Printf("%s: got error event...aborting\n", e.eventSourceID)
//					e.setReason(evt)
//					e.setState(pipeline.EventSourceStateAborted)
//					return
//				case pipeline.DataEvent[I]:
//					//fmt.Printf("%s: processing data\n", e.eventSourceID)
//					data, err := e.processor.Process(evt.Data())
//					if err != nil {
//						//fmt.Printf("%s: got processing data error %s\n", e.eventSourceID, err)
//						errEvent := e.eventFactory.NewErrorEvent(err)
//						sendEvent(ctx, e.outputChannel, errEvent)
//						e.setReason(errEvent)
//						e.setState(pipeline.EventSourceStateFailed)
//						return
//					}
//					if data != nil {
//						func() {
//							e.lock.RLock()
//							defer e.lock.RUnlock()
//							dataEvent := e.eventFactory.NewDataEvent(*data)
//							outputIds := e.outputs.EventSourceIDs()
//							for i := 0; i < len(outputIds); i++ {
//								evt := dataEvent.Copy()
//								evt.Route(outputIds[i])
//								//fmt.Printf("%s: emitting event to %s\n", e.eventSourceID, outputIds[i])
//								sendEvent(ctx, e.outputChannel, evt)
//							}
//						}()
//					}
//				}
//			}
//		}
//	}()
//}
//
//func (e *EventProcessor[I]) IngressCapacity() int {
//	return e.ingressCapacity
//}
//
//func (e *EventProcessor[I]) EventSourceID() pipeline.EventSourceID {
//	return e.eventSourceID
//}
//
//func sendEvent(ctx context.Context, outputChannel chan<- pipeline.Event, event pipeline.Event) {
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case outputChannel <- event:
//			return
//		}
//	}
//}
