package eventbus

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type EventType string
type EventID string
type EventMetadata map[string]string

const (
	EventTypeData  EventType = "DATA"
	EventTypeError EventType = "ERROR"
)

type EventVisit interface {
	EventSourceID() EventSourceID
	VisitationTime() time.Time
}

var _ EventVisit = &eventVisit{}

type eventVisit struct {
	VisitEventSourceID EventSourceID `json:"eventSourceID"`
	Time               time.Time     `json:"visitationTime"`
}

func (p *eventVisit) EventSourceID() EventSourceID {
	return p.VisitEventSourceID
}

func (p *eventVisit) VisitationTime() time.Time {
	return p.Time
}

type EventHeader interface {
	EventID() EventID
	ParentEventID() EventID
	Creator() EventSourceID
	Sender() EventSourceID
	Receiver() EventSourceID
	IsBroadcastEvent() bool
	CreationTime() time.Time
	Visited() []EventVisit
	Metadata() EventMetadata
	registerVisitor(eventSourceID EventSourceID)
	route(receiver EventSourceID)
	setParentEventID(eventID EventID)
	broadcast()
	Copy() EventHeader
}

var _ EventHeader = &eventHeader{}

type eventHeader struct {
	HeaderEventID          EventID       `json:"eventID"`
	HeaderParentEventID    EventID       `json:"parentPipelineEventID,omitempty"`
	HeaderCreator          EventSourceID `json:"creatorEventSourceID"`
	HeaderSender           EventSourceID `json:"sender"`
	HeaderReceiver         EventSourceID `json:"receiver,omitempty"`
	HeaderIsBroadcastEvent bool          `json:"broadcast"`
	HeaderCreationTime     time.Time     `json:"creationTime"`
	HeaderVisited          []EventVisit  `json:"visited"`
	HeaderEventMetadata    EventMetadata `json:"metadata,omitempty"`
	lock                   sync.RWMutex
}

func (p *eventHeader) route(receiver EventSourceID) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.HeaderIsBroadcastEvent = false
	p.HeaderReceiver = receiver
}

func (p *eventHeader) registerVisitor(eventSourceID EventSourceID) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.HeaderVisited = append(p.HeaderVisited, &eventVisit{
		VisitEventSourceID: eventSourceID,
		Time:               time.Now(),
	})
}

func (p *eventHeader) setParentEventID(eventID EventID) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.HeaderParentEventID = eventID
}

func (p *eventHeader) broadcast() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.HeaderReceiver = ""
	p.HeaderIsBroadcastEvent = true
}

func (p *eventHeader) Copy() EventHeader {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return &eventHeader{
		HeaderEventID:          p.HeaderEventID,
		HeaderParentEventID:    p.HeaderParentEventID,
		HeaderSender:           p.HeaderSender,
		HeaderVisited:          p.HeaderVisited,
		HeaderCreator:          p.HeaderCreator,
		HeaderReceiver:         p.HeaderReceiver,
		HeaderIsBroadcastEvent: p.HeaderIsBroadcastEvent,
		HeaderEventMetadata:    p.HeaderEventMetadata,
		HeaderCreationTime:     p.HeaderCreationTime,
	}
}

func (p *eventHeader) EventID() EventID {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderEventID
}

func (p *eventHeader) ParentEventID() EventID {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderParentEventID
}

func (p *eventHeader) Creator() EventSourceID {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderCreator
}

func (p *eventHeader) Sender() EventSourceID {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderSender
}

func (p *eventHeader) Receiver() EventSourceID {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderReceiver
}

func (p *eventHeader) IsBroadcastEvent() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderIsBroadcastEvent
}

func (p *eventHeader) CreationTime() time.Time {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderCreationTime
}

func (p *eventHeader) Visited() []EventVisit {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderVisited
}

func (p *eventHeader) Metadata() EventMetadata {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.HeaderEventMetadata
}

type Event interface {
	Header() EventHeader
	Broadcast()
	Route(dest EventSourceID)
	Visit(visitor EventSourceID)
	String() string
}

var _ Event = &event{}

type event struct {
	EventHeader EventHeader `json:"header"`
	lock        sync.RWMutex
}

func (e *event) Header() EventHeader {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.EventHeader
}

func (e *event) Broadcast() {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.EventHeader.broadcast()
}

func (e *event) Route(dest EventSourceID) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.EventHeader.route(dest)
}

func (e *event) Visit(visitor EventSourceID) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.EventHeader.registerVisitor(visitor)
}

func (e *event) String() string {
	e.lock.Lock()
	defer e.lock.Unlock()
	bytes, err := json.Marshal(e)
	if err != nil {
		return string(e.EventHeader.EventID())
	}
	return string(bytes)
}

type DataEvent[D interface{}] interface {
	Event
	Data() D
	Copy() DataEvent[D]
}

type ErrorEvent interface {
	Event
	Error() error
	Copy() ErrorEvent
}

var _ DataEvent[interface{}] = &dataEvent[interface{}]{}

type dataEvent[D interface{}] struct {
	*event
	EventData D `json:"data"`
}

func (p *dataEvent[I]) Header() EventHeader {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.EventHeader
}

func (p *dataEvent[I]) Data() I {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.EventData
}

func (p *dataEvent[I]) String() string {
	p.lock.Lock()
	defer p.lock.Unlock()
	bytes, err := json.Marshal(p)
	if err != nil {
		return string(p.EventHeader.EventID())
	}
	return string(bytes)
}

func (p *dataEvent[I]) Copy() DataEvent[I] {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return &dataEvent[I]{
		event: &event{
			EventHeader: p.Header().Copy(),
			lock:        sync.RWMutex{},
		},
		EventData: p.EventData,
	}
}

var _ ErrorEvent = &errorEvent{}

type errorEvent struct {
	*event
	EventError error `json:"error"`
}

func (e *errorEvent) Header() EventHeader {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.EventHeader
}

func (e *errorEvent) Error() error {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.EventError
}

func (e *errorEvent) String() string {
	e.lock.Lock()
	defer e.lock.Unlock()
	bytes, err := json.Marshal(e)
	if err != nil {
		return string(e.EventHeader.EventID())
	}
	return string(bytes)
}

func (e *errorEvent) Copy() ErrorEvent {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return &errorEvent{
		event: &event{
			EventHeader: e.Header().Copy(),
			lock:        sync.RWMutex{},
		},
		EventError: e.EventError,
	}
}

type EventIDProvider interface {
	NextEventID() EventID
}

type UUIDProviderFn func() (uuid.UUID, error)

type UUIDEventIDProvider struct {
	nextUUIDFn UUIDProviderFn
}

func NewUUIDEventIDProvider() *UUIDEventIDProvider {
	return &UUIDEventIDProvider{
		nextUUIDFn: func() (uuid.UUID, error) { return uuid.NewRandom() },
	}
}

func NewCustomUUIDEventIDProvider(nextUUIDFn UUIDProviderFn) *UUIDEventIDProvider {
	return &UUIDEventIDProvider{
		nextUUIDFn: nextUUIDFn,
	}
}

func (p *UUIDEventIDProvider) NextEventID() EventID {
	eid, err := p.nextUUIDFn()
	if err != nil {
		id := err.Error() + time.Now().String()
		id = hex.EncodeToString([]byte(id))
		return EventID(fmt.Sprintf("%s (with error: %s)", id, err))
	}
	return EventID(hex.EncodeToString([]byte(eid.String())))
}

type EventFactory[I interface{}] interface {
	NewErrorEvent(err error) ErrorEvent
	NewDataEvent(data I) DataEvent[I]
	NewMutatedDataEvent(oldDataEvent DataEvent[I], newData I) DataEvent[I]
}

var _ EventFactory[interface{}] = &eventFactory[interface{}]{}

type EventFactoryOption[I interface{}] func(factory *eventFactory[I])

func WithEventIDProvider[I interface{}](eventIDProvider EventIDProvider) EventFactoryOption[I] {
	return func(eventFactory *eventFactory[I]) {
		eventFactory.eventIDProvider = eventIDProvider
	}
}

func WithEventMetadata[I interface{}](eventMetadata EventMetadata) EventFactoryOption[I] {
	return func(eventFactory *eventFactory[I]) {
		eventFactory.eventMetadata = eventMetadata
	}
}

type eventFactory[I interface{}] struct {
	eventSourceID   EventSourceID
	eventMetadata   EventMetadata
	eventIDProvider EventIDProvider
}

func NewEventFactory[I interface{}](eventSourceID EventSourceID, options ...EventFactoryOption[I]) EventFactory[I] {
	factory := &eventFactory[I]{
		eventSourceID:   eventSourceID,
		eventIDProvider: NewUUIDEventIDProvider(),
	}

	for _, applyOption := range options {
		applyOption(factory)
	}

	return factory
}

func (p *eventFactory[I]) NewErrorEvent(err error) ErrorEvent {
	return &errorEvent{
		event:      &event{EventHeader: p.newEventHeader(EventTypeData), lock: sync.RWMutex{}},
		EventError: err,
	}
}

func (p *eventFactory[I]) NewDataEvent(data I) DataEvent[I] {
	return &dataEvent[I]{
		event:     &event{EventHeader: p.newEventHeader(EventTypeData), lock: sync.RWMutex{}},
		EventData: data,
	}
}

func (p *eventFactory[I]) NewMutatedDataEvent(oldDataEvent DataEvent[I], newData I) DataEvent[I] {
	event := p.NewDataEvent(newData)
	event.Header().setParentEventID(oldDataEvent.Header().EventID())
	return event
}

func (p *eventFactory[I]) newEventHeader(eventType EventType) EventHeader {
	return &eventHeader{
		HeaderEventMetadata:    p.eventMetadata,
		HeaderCreator:          p.eventSourceID,
		HeaderSender:           p.eventSourceID,
		HeaderCreationTime:     time.Now(),
		HeaderEventID:          p.eventIDProvider.NextEventID(),
		HeaderIsBroadcastEvent: eventType == EventTypeError,
		HeaderVisited:          []EventVisit{},
		lock:                   sync.RWMutex{},
	}
}
