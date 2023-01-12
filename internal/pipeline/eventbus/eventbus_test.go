package eventbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/pipeline/event"
	"github.com/perdasilva/olmcli/internal/pipeline/eventbus"
	"github.com/perdasilva/olmcli/internal/pipeline/eventbus/eventrouter"
	"github.com/stretchr/testify/assert"
)

var _ pipeline.EventSource = &dummyEventSource{}

type dummyEventSource struct {
	ingressCapacity int
	eventSourceID   pipeline.EventSourceID
	eventFactory    pipeline.EventFactory[int]
	in              <-chan pipeline.Event
	out             chan<- pipeline.Event
}

func newEventSource(eventSourceID pipeline.EventSourceID, ingressCapacity int) *dummyEventSource {
	return &dummyEventSource{
		eventSourceID:   eventSourceID,
		ingressCapacity: ingressCapacity,
		eventFactory:    event.NewEventFactory[int](eventSourceID),
	}
}

func (d *dummyEventSource) Connect(bus pipeline.EventBus) *dummyEventSource {
	d.in, d.out = bus.Connect(d)
	return d
}

func (d *dummyEventSource) IngressCapacity() int {
	return d.ingressCapacity
}

func (d *dummyEventSource) EventSourceID() pipeline.EventSourceID {
	return d.eventSourceID
}

func newTestContext() (context.Context, context.CancelFunc) {
	return context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
}

func TestEventBus_routes(t *testing.T) {
	testCtx, cancel := newTestContext()
	defer cancel()
	bus := eventbus.NewEventBus(testCtx)
	sender := newEventSource("sender", 0).Connect(bus)
	receiver := newEventSource("receiver", 0).Connect(bus)

	// sender
	sentEventCh := make(chan pipeline.Event)
	go func(sentEventCh chan<- pipeline.Event) {
		event := sender.eventFactory.NewDataEvent(1)
		event.Route("receiver")
		for {
			select {
			case <-testCtx.Done():
				return
			case sender.out <- event:
				sentEventCh <- event
				close(sender.out)
				return
			}
		}
	}(sentEventCh)

	// receiver
	receivedEventCh := make(chan pipeline.Event)
	go func(receivedEventCh chan<- pipeline.Event) {
		for {
			select {
			case <-testCtx.Done():
				return
			case evt := <-receiver.in:
				receivedEventCh <- evt
				close(receiver.out)
				return
			}
		}
	}(receivedEventCh)

	sentEvent := <-sentEventCh
	receivedEvent := <-receivedEventCh
	assert.Equal(t, sentEvent, receivedEvent)
}

func TestEventBus_broadcasts(t *testing.T) {
	testCtx, cancel := newTestContext()
	defer cancel()
	bus := eventbus.NewEventBus(testCtx)
	sender := newEventSource("sender", 0).Connect(bus)
	nodeOne := newEventSource("nodeOne", 0).Connect(bus)
	nodeTwo := newEventSource("nodeTwo", 0).Connect(bus)
	nodeThree := newEventSource("nodeThree", 0).Connect(bus)

	// sender
	sentEventCh := make(chan pipeline.Event)
	go func(sentEventCh chan<- pipeline.Event) {
		evt := sender.eventFactory.NewDataEvent(1)
		evt.Broadcast()
		for {
			select {
			case <-testCtx.Done():
				return
			case sender.out <- evt:
				sentEventCh <- evt
				close(sender.out)
				<-sender.in
				return
			}
		}
	}(sentEventCh)

	// receivers
	receivedEventCh := make(chan pipeline.Event, 3)
	for _, node := range []*dummyEventSource{nodeOne, nodeTwo, nodeThree} {
		go func(node *dummyEventSource, receivedEventCh chan<- pipeline.Event) {
			for {
				select {
				case <-testCtx.Done():
					return
				case evt := <-node.in:
					receivedEventCh <- evt
					close(node.out)
					return
				}
			}
		}(node, receivedEventCh)
	}

	// TODO: should watch context
	sentEvent := <-sentEventCh
	outEvent1 := <-receivedEventCh
	outEvent2 := <-receivedEventCh
	outEvent3 := <-receivedEventCh

	assert.Equal(t, sentEvent.Header().EventID(), outEvent1.Header().EventID())
	assert.Equal(t, sentEvent.Header().EventID(), outEvent2.Header().EventID())
	assert.Equal(t, sentEvent.Header().EventID(), outEvent3.Header().EventID())
}

func TestEventBus_cancels(t *testing.T) {
	bus := eventbus.NewEventBus(context.Background())
	sender := newEventSource("sender", 0).Connect(bus)
	receiver := newEventSource("receiver", 0).Connect(bus)

	// sender
	go func() {
		for i := 0; i < 20; i++ {
			evt := sender.eventFactory.NewDataEvent(1)
			evt.Route("receiver")
			sender.out <- evt
		}
		close(sender.out)
	}()

	// receiver will stop the bus after 10 messages
	done := make(chan int)
	go func(done chan<- int) {
		i := 0
		for {
			select {
			case _, hasNext := <-receiver.in:
				if !hasNext {
					close(receiver.out)
					done <- i
					return
				}
				i = i + 1
				if i == 10 {
					bus.Stop()
				}
			}
		}
	}(done)
	receivedCount := <-done
	assert.Equal(t, 10, receivedCount)
}

func TestEventBus_debugs(t *testing.T) {
	debugChannel := make(chan pipeline.Event)
	bus := eventbus.NewEventBus(context.Background(), eventrouter.WithDebugChannel(debugChannel))
	sender := newEventSource("sender", 0).Connect(bus)
	receiver := newEventSource("receiver", 0).Connect(bus)
	numMessages := 5

	// sender
	go func() {
		for i := 0; i < numMessages; i++ {
			evt := sender.eventFactory.NewDataEvent(i)
			evt.Route("receiver")
			sender.out <- evt
		}
		close(sender.out)
	}()

	// receiver
	go func() {
		for i := 0; i < numMessages; i++ {
			<-receiver.in
		}
		bus.Stop()
		close(receiver.out)
	}()

	var events []pipeline.Event
	func() {
		for {
			select {
			case evt, hasNext := <-debugChannel:
				if !hasNext {
					return
				}
				events = append(events, evt)
			}
		}
	}()
	assert.Len(t, events, numMessages)
}
