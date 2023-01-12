package event

import (
	"sync"
	"time"

	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/pipeline/event/eventidprovider"
)

var _ pipeline.EventFactory[interface{}] = &eventFactory[interface{}]{}

type EventFactoryOption[I interface{}] func(factory *eventFactory[I])

func WithEventIDProvider[I interface{}](eventIDProvider pipeline.EventIDProvider) EventFactoryOption[I] {
	return func(eventFactory *eventFactory[I]) {
		eventFactory.eventIDProvider = eventIDProvider
	}
}

func WithEventMetadata[I interface{}](eventMetadata pipeline.EventMetadata) EventFactoryOption[I] {
	return func(eventFactory *eventFactory[I]) {
		eventFactory.eventMetadata = eventMetadata
	}
}

type eventFactory[I interface{}] struct {
	eventSourceID   pipeline.EventSourceID
	eventMetadata   pipeline.EventMetadata
	eventIDProvider pipeline.EventIDProvider
}

func NewEventFactory[I interface{}](eventSourceID pipeline.EventSourceID, options ...EventFactoryOption[I]) pipeline.EventFactory[I] {
	factory := &eventFactory[I]{
		eventSourceID: eventSourceID,
		// eventIDProvider: eventidprovider.NewUUIDEventIDProvider(),
		eventIDProvider: eventidprovider.MonotonicallyIncreasingEventIDProvider(),
	}

	for _, applyOption := range options {
		applyOption(factory)
	}

	return factory
}

func (p *eventFactory[I]) NewErrorEvent(err error) pipeline.ErrorEvent {
	return &errorEvent{
		event:      newEvent(p.newEventHeader()),
		EventError: err,
	}
}

func (p *eventFactory[I]) NewDataEvent(data I) pipeline.DataEvent[I] {
	return &dataEvent[I]{
		event:     newEvent(p.newEventHeader()),
		EventData: data,
	}
}

func (p *eventFactory[I]) NewMutatedDataEvent(oldDataEvent pipeline.DataEvent[I], newData I) pipeline.DataEvent[I] {
	event := &dataEvent[I]{
		event:     newEvent(p.newEventHeader()),
		EventData: newData,
	}
	event.EventHeader.setParentEventID(oldDataEvent.Header().EventID())
	return event
}

func (p *eventFactory[I]) newEventHeader() *eventHeader {
	return &eventHeader{
		HeaderEventMetadata:    p.eventMetadata,
		HeaderCreator:          p.eventSourceID,
		HeaderSender:           p.eventSourceID,
		HeaderCreationTime:     time.Now(),
		HeaderEventID:          p.eventIDProvider.NextEventID(),
		HeaderIsBroadcastEvent: false,
		HeaderVisited:          []pipeline.EventVisit{},
		lock:                   sync.RWMutex{},
	}
}
