package eventbus_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/perdasilva/olmcli/internal/eventbus"
	"github.com/stretchr/testify/assert"
)

var _ eventbus.Producer[int] = &intProducer{}

type intProducer struct {
	values []int
	index  int
}

func newIntProducer(values []int) *intProducer {
	return &intProducer{
		values: values,
		index:  0,
	}
}

func (i *intProducer) Produce() (*int, error) {
	if i.index == len(i.values) {
		return nil, nil
	}
	value := i.values[i.index]
	i.index = i.index + 1
	return &value, nil
}

type nilProducer struct {
	values   []int
	index    int
	nilIndex int
}

func newNilProducer(values []int, nilIndex int) *nilProducer {
	return &nilProducer{
		values:   values,
		nilIndex: nilIndex,
		index:    0,
	}
}

func (i *nilProducer) Produce() (*int, error) {
	if i.index == len(i.values) {
		return nil, nil
	}
	if i.index == i.nilIndex {
		return nil, nil
	}
	value := i.values[i.index]
	i.index = i.index + 1
	return &value, nil
}

type errorProducer struct {
	err error
}

func (i *errorProducer) Produce() (*int, error) {
	return nil, i.err
}

var _ eventbus.Consumer[int] = &intConsumer{}

type intConsumer struct {
	values []int
}

func (i *intConsumer) Consume(value int) error {
	i.values = append(i.values, value)
	return nil
}

type errorConsumer struct {
	err error
}

func (i *errorConsumer) Consume(_ int) error {
	return i.err
}

var _ eventbus.Processor[int] = &intDoubler{}

type intDoubler struct {
}

func (i *intDoubler) InputFinished() (<-chan int, <-chan error) {
	return nil, nil
}

func (i *intDoubler) Process(value int) (*int, error) {
	d := value * 2
	return &d, nil
}

type intAdder struct {
	sum int
}

func (i *intAdder) InputFinished() (<-chan int, <-chan error) {
	dataOut := make(chan int)
	go func() {
		dataOut <- i.sum
		close(dataOut)
	}()
	return dataOut, nil
}

func (i *intAdder) Process(value int) (*int, error) {
	i.sum = i.sum + value
	return nil, nil
}

type errorProcessor struct {
	err error
}

func (i *errorProcessor) InputFinished() (<-chan int, <-chan error) {
	return nil, nil
}

func (i *errorProcessor) Process(value int) (*int, error) {
	return nil, i.err
}

type inputFinishedErrorProcessor struct {
	err error
}

func (i *inputFinishedErrorProcessor) InputFinished() (<-chan int, <-chan error) {
	errChan := make(chan error)
	go func() {
		errChan <- i.err
		close(errChan)
	}()
	return nil, errChan
}

func (i *inputFinishedErrorProcessor) Process(value int) (*int, error) {
	return nil, nil
}

var _ eventbus.EventBus = &fakeEventBus{}

type fakeEventBus struct {
	inputChannel  chan eventbus.Event
	outputChannel chan eventbus.Event
}

func (f *fakeEventBus) AutoClosDestination(src eventbus.EventSourceID, dests ...eventbus.EventSourceID) {

}

func (f *fakeEventBus) Connect(eventSource eventbus.EventSource) (<-chan eventbus.Event, chan<- eventbus.Event) {
	f.inputChannel = make(chan eventbus.Event, eventSource.IngressCapacity())
	f.outputChannel = make(chan eventbus.Event)
	return f.inputChannel, f.outputChannel
}

func TestEventProducer_producesSingleOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	eventProducer := eventbus.NewEventProducer[int]("test", newIntProducer([]int{1, 2, 3, 4}), &bus)
	eventProducer.AddOutput("receiver")
	assert.Equal(t, eventbus.EventSourceStateInactive, eventProducer.State())
	eventProducer.Start(ctx)
	assert.Equal(t, eventbus.EventSourceStateActive, eventProducer.State())
	var result []int
	func() {
		for {
			select {
			case event, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := event.(type) {
				case eventbus.DataEvent[int]:
					result = append(result, evt.Data())
				}
			}
		}
	}()
	assert.Equal(t, []int{1, 2, 3, 4}, result)
	assert.Equal(t, eventbus.EventSourceStateSuccess, eventProducer.State())
}

func TestEventProducer_producesMultiOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	outputs := []eventbus.EventSourceID{"r1", "r2", "r3"}
	eventProducer := eventbus.NewEventProducer[int]("test", newIntProducer([]int{1, 2}), &bus)
	eventProducer.AddOutput(outputs...)
	assert.Equal(t, eventbus.EventSourceStateInactive, eventProducer.State())
	eventProducer.Start(ctx)
	assert.Equal(t, eventbus.EventSourceStateActive, eventProducer.State())

	result := map[eventbus.EventSourceID]map[int]eventbus.EventID{}
	func() {
		for {
			select {
			case event, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := event.(type) {
				case eventbus.DataEvent[int]:
					if _, ok := result[evt.Header().Receiver()]; !ok {
						result[evt.Header().Receiver()] = map[int]eventbus.EventID{}
					}
					result[evt.Header().Receiver()][evt.Data()] = evt.Header().EventID()
				}
			}
		}
	}()

	// check that the event ids for the same data across receivers is the same
	// result[receiverID][data] = eventID
	assert.Equal(t, result["r1"][1], result["r2"][1])
	assert.Equal(t, result["r2"][1], result["r3"][1])
	assert.Equal(t, result["r1"][2], result["r2"][2])
	assert.Equal(t, result["r2"][2], result["r3"][2])
	assert.Equal(t, eventbus.EventSourceStateSuccess, eventProducer.State())
}

func TestEventProducer_bailsOnErrorWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	eventProducer := eventbus.NewEventProducer[int]("test", &errorProducer{fmt.Errorf("some error")}, &bus)
	eventProducer.Start(ctx)
	var err error
	for event, hasNext := <-bus.outputChannel; hasNext; event, hasNext = <-bus.outputChannel {
		switch evt := event.(type) {
		case eventbus.ErrorEvent:
			err = evt.Error()
		}
	}
	assert.Equal(t, fmt.Errorf("some error"), err)
	assert.Equal(t, eventbus.EventSourceStateFailed, eventProducer.State())
}

func TestEventProducer_bailsOnErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	eventProducer := eventbus.NewEventProducer[int]("test", newIntProducer([]int{1, 2, 3, 4}), &bus)
	eventProducer.Start(ctx)
	eventFactory := eventbus.NewEventFactory[int]("test")
	bus.inputChannel <- eventFactory.NewErrorEvent(fmt.Errorf("some error"))
	close(bus.inputChannel)
	assert.NotNil(t, eventProducer.Reason())
	assert.Equal(t, fmt.Errorf("some error"), eventProducer.Reason().Error())
	assert.Equal(t, eventbus.EventSourceStateAborted, eventProducer.State())
}

func TestEventConsumer_consumes(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	consumer := &intConsumer{}
	eventConsumer := eventbus.NewEventConsumer[int]("test", consumer, &bus)
	assert.Equal(t, eventbus.EventSourceStateInactive, eventConsumer.State())
	eventConsumer.Start(ctx)
	assert.Equal(t, eventbus.EventSourceStateActive, eventConsumer.State())
	eventFactory := eventbus.NewEventFactory[int]("test")
	for _, value := range []int{1, 2, 3, 4} {
		bus.inputChannel <- eventFactory.NewDataEvent(value)
	}
	close(bus.inputChannel)
	<-eventConsumer.Done()
	assert.Equal(t, []int{1, 2, 3, 4}, consumer.values)
	assert.Equal(t, eventbus.EventSourceStateSuccess, eventConsumer.State())
}

func TestEventConsumer_bailsOnErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	consumer := &intConsumer{}
	eventConsumer := eventbus.NewEventConsumer[int]("test", consumer, &bus)
	eventConsumer.Start(ctx)
	eventFactory := eventbus.NewEventFactory[int]("test")
	bus.inputChannel <- eventFactory.NewErrorEvent(fmt.Errorf("some error"))
	close(bus.inputChannel)
	<-eventConsumer.Done()
	assert.Nil(t, consumer.values)
	assert.NotNil(t, eventConsumer.Reason())
	assert.Equal(t, fmt.Errorf("some error"), eventConsumer.Reason().Error())
	assert.Equal(t, eventbus.EventSourceStateAborted, eventConsumer.State())
}

func TestEventConsumer_bailsOnErrorWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	consumer := &errorConsumer{fmt.Errorf("some error")}
	eventConsumer := eventbus.NewEventConsumer[int]("test", consumer, &bus)
	eventConsumer.Start(ctx)
	eventFactory := eventbus.NewEventFactory[int]("test")

	errChan := make(chan eventbus.ErrorEvent)
	go func(errChan chan eventbus.ErrorEvent) {
		defer func() {
			close(errChan)
		}()

		bus.inputChannel <- eventFactory.NewDataEvent(1)
		for {
			select {
			case <-ctx.Done():
				return
			case event, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := event.(type) {
				case eventbus.ErrorEvent:
					errChan <- evt
					return
				}
			}
		}
	}(errChan)
	<-eventConsumer.Done()
	errorEvent := <-errChan
	assert.NotNil(t, errorEvent)
	assert.NotNil(t, eventConsumer.Reason())
	assert.Equal(t, fmt.Errorf("some error"), errorEvent.Error())
	assert.Equal(t, eventbus.EventSourceStateFailed, eventConsumer.State())
}

func TestEventProcessor_processesSingleOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intDoubler{}
	eventProcessor := eventbus.NewEventProcessor[int]("test", processor, &bus)
	eventProcessor.AddOutput("receiver")
	assert.Equal(t, eventbus.EventSourceStateInactive, eventProcessor.State())
	eventProcessor.Start(ctx)
	assert.Equal(t, eventbus.EventSourceStateActive, eventProcessor.State())
	eventFactory := eventbus.NewEventFactory[int]("test")

	// produce some input
	go func() {
		for _, value := range []int{1, 2, 3, 4} {
			bus.inputChannel <- eventFactory.NewDataEvent(value)
		}
		close(bus.inputChannel)
	}()

	// capture output
	var result []int
	for event, hasNext := <-bus.outputChannel; hasNext; event, hasNext = <-bus.outputChannel {
		switch evt := event.(type) {
		case eventbus.DataEvent[int]:
			result = append(result, evt.Data())
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, []int{2, 4, 6, 8}, result)
	assert.Equal(t, eventbus.EventSourceStateSuccess, eventProcessor.State())
}

func TestEventProcessor_processesMultiOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intDoubler{}
	outputs := []eventbus.EventSourceID{"r1", "r2", "r3"}
	eventProcessor := eventbus.NewEventProcessor[int]("test", processor, &bus)
	eventProcessor.AddOutput(outputs...)
	assert.Equal(t, eventbus.EventSourceStateInactive, eventProcessor.State())
	eventProcessor.Start(ctx)
	assert.Equal(t, eventbus.EventSourceStateActive, eventProcessor.State())
	eventFactory := eventbus.NewEventFactory[int]("test")

	// produce some input
	go func() {
		for _, value := range []int{1, 2} {
			bus.inputChannel <- eventFactory.NewDataEvent(value)
		}
		close(bus.inputChannel)
	}()

	result := map[eventbus.EventSourceID]map[int]eventbus.EventID{}
	func() {
		for {
			select {
			case event, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := event.(type) {
				case eventbus.DataEvent[int]:
					if _, ok := result[evt.Header().Receiver()]; !ok {
						result[evt.Header().Receiver()] = map[int]eventbus.EventID{}
					}
					result[evt.Header().Receiver()][evt.Data()] = evt.Header().EventID()
				}
			}
		}
	}()

	// check that the event ids for the same data across receivers is the same
	// result[receiverID][data] = eventID
	assert.Equal(t, result["r1"][1], result["r2"][1])
	assert.Equal(t, result["r2"][1], result["r3"][1])
	assert.Equal(t, result["r1"][2], result["r2"][2])
	assert.Equal(t, result["r2"][2], result["r3"][2])
	assert.Equal(t, eventbus.EventSourceStateSuccess, eventProcessor.State())
}

func TestEventProcessor_callsInputFinishedSingleOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intAdder{}
	eventProcessor := eventbus.NewEventProcessor[int]("test", processor, &bus)
	eventProcessor.AddOutput("receiver")
	eventProcessor.Start(ctx)
	eventFactory := eventbus.NewEventFactory[int]("test")

	// produce some input
	go func() {
		for _, value := range []int{1, 2, 3, 4} {
			bus.inputChannel <- eventFactory.NewDataEvent(value)
		}
		close(bus.inputChannel)
	}()

	// capture output
	var result []int
	for event, hasNext := <-bus.outputChannel; hasNext; event, hasNext = <-bus.outputChannel {
		switch evt := event.(type) {
		case eventbus.DataEvent[int]:
			result = append(result, evt.Data())
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, []int{10}, result)
	assert.Equal(t, eventbus.EventSourceStateSuccess, eventProcessor.State())
}

func TestEventProcessor_bailsOnErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intDoubler{}
	eventProcessor := eventbus.NewEventProcessor[int]("test", processor, &bus)
	eventProcessor.Start(ctx)
	eventFactory := eventbus.NewEventFactory[int]("test")
	bus.inputChannel <- eventFactory.NewErrorEvent(fmt.Errorf("some error"))
	<-eventProcessor.Done()
	assert.NotNil(t, eventProcessor.Reason())
	assert.Equal(t, fmt.Errorf("some error"), eventProcessor.Reason().Error())
	assert.Equal(t, eventbus.EventSourceStateAborted, eventProcessor.State())
}

func TestEventProcessor_bailsOnErrorWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &errorProcessor{fmt.Errorf("some error")}
	eventProcessor := eventbus.NewEventProcessor[int]("test", processor, &bus)
	eventProcessor.Start(ctx)
	eventFactory := eventbus.NewEventFactory[int]("test")

	// produce some input
	go func() {
		bus.inputChannel <- eventFactory.NewDataEvent(1)
		close(bus.inputChannel)
	}()

	// capture output
	var errEvent eventbus.ErrorEvent
	for event, hasNext := <-bus.outputChannel; hasNext; event, hasNext = <-bus.outputChannel {
		switch evt := event.(type) {
		case eventbus.ErrorEvent:
			errEvent = evt
			return
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, fmt.Errorf("some error"), errEvent.Error())
	assert.Equal(t, eventbus.EventSourceStateFailed, eventProcessor.State())
}

func TestEventProcessor_bailsOnErrorOnInputFinishedWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &inputFinishedErrorProcessor{fmt.Errorf("some error")}
	eventProcessor := eventbus.NewEventProcessor[int]("test", processor, &bus)
	eventProcessor.Start(ctx)
	eventFactory := eventbus.NewEventFactory[int]("test")

	// produce some input
	go func() {
		for _, value := range []int{1, 2, 3, 4} {
			bus.inputChannel <- eventFactory.NewDataEvent(value)
		}
		close(bus.inputChannel)
	}()

	// capture output
	var errEvent eventbus.ErrorEvent
	for event, hasNext := <-bus.outputChannel; hasNext; event, hasNext = <-bus.outputChannel {
		switch evt := event.(type) {
		case eventbus.ErrorEvent:
			errEvent = evt
			return
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, fmt.Errorf("some error"), errEvent.Error())
	assert.Equal(t, eventbus.EventSourceStateFailed, eventProcessor.State())
}
