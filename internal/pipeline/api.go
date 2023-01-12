package pipeline

import (
	"context"
	"time"
)

type EventSourceID string
type EventSourceState string

type EventSourceSet map[EventSourceID]struct{}

func (o EventSourceSet) EventSourceIDs() []EventSourceID {
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
	EventSourceID() EventSourceID
	IngressCapacity() int
}

type OutputProducingEventSource interface {
	AddOutput(dest ...EventSourceID)
}

type ProducerEventSource interface {
	EventSource
	OutputProducingEventSource
	Start(ctx context.Context)
	State() EventSourceState
	Done() <-chan struct{}
	Reason() ErrorEvent
}

type ProcessorEventSource interface {
	EventSource
	OutputProducingEventSource
	Start(ctx context.Context)
	State() EventSourceState
	Done() <-chan struct{}
	Reason() ErrorEvent
}

type ConsumerEventSource interface {
	EventSource
	Start(ctx context.Context)
	State() EventSourceState
	Done() <-chan struct{}
	Reason() ErrorEvent
}

type Process[C ProcessContext] interface {
	Execute(ctx C)
}

type Producer[D interface{}] interface {
	Process[ProducerContext[D]]
}

type Consumer[D interface{}] interface {
	Process[ConsumerContext[D]]
}

type Processor[D interface{}] interface {
	Process[ProcessorContext[D]]
}

type EventBus interface {
	Connect(eventSource EventSource) (<-chan Event, chan<- Event)
	AutoClosDestination(src EventSourceID, dests ...EventSourceID)
}

type EventType string
type EventID string
type EventMetadata map[string]string

type EventVisit interface {
	EventSourceID() EventSourceID
	VisitationTime() time.Time
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
}

type EventIDProvider interface {
	NextEventID() EventID
}

type Event interface {
	Header() EventHeader
	Broadcast()
	Route(dest EventSourceID)
	Visit(visitor EventSourceID)
	String() string
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

type EventFactory[I interface{}] interface {
	NewErrorEvent(err error) ErrorEvent
	NewDataEvent(data I) DataEvent[I]
	NewMutatedDataEvent(oldDataEvent DataEvent[I], newData I) DataEvent[I]
}

type ProcessContext interface {
	Error() error
	Close()
	CloseWithError(err error)
}

type ProducerContext[D interface{}] interface {
	ProcessContext
	Write(data D)
}

type ConsumerContext[D interface{}] interface {
	ProcessContext
	Read() (D, bool)
}

type ProcessorContext[D interface{}] interface {
	ProcessContext
	ProducerContext[D]
	ConsumerContext[D]
}
