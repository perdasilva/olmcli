package v2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/pipeline/event"
	"github.com/stretchr/testify/assert"
)

var _ pipeline.Producer[int] = &intProducer{}

type intProducer struct {
	values []int
}

func (i *intProducer) Execute(ctx pipeline.ProducerContext[int]) {
	for _, value := range i.values {
		fmt.Printf("emmitting %d\n", value)
		ctx.Write(value)
	}
	ctx.Close()
}

func newIntProducer(values []int) *intProducer {
	return &intProducer{
		values: values,
	}
}

var _ pipeline.Producer[int] = &errorProducer{}

type errorProducer struct {
	err error
}

func (i *errorProducer) Execute(ctx pipeline.ProducerContext[int]) {
	ctx.CloseWithError(i.err)
}

var _ pipeline.Consumer[int] = &intConsumer{}

type intConsumer struct {
	values []int
}

func (i *intConsumer) Execute(ctx pipeline.ConsumerContext[int]) {
	for {
		data, hasNext := ctx.Read()
		if !hasNext {
			fmt.Println("int consumer ran out of input - closing")
			ctx.Close()
			return
		}
		fmt.Printf("got data %d\n", data)
		i.values = append(i.values, data)
	}
}

var _ pipeline.Consumer[int] = &errorConsumer{}

type errorConsumer struct {
	err error
}

func (i *errorConsumer) Execute(ctx pipeline.ConsumerContext[int]) {
	ctx.CloseWithError(i.err)
}

var _ pipeline.Processor[int] = &intDoubler{}

type intDoubler struct {
}

func (i *intDoubler) Execute(ctx pipeline.ProcessorContext[int]) {
	for {
		fmt.Println("id: waiting for data")
		data, hasNext := ctx.Read()
		if !hasNext {
			fmt.Println("id: int doubler closing due to end of data")
			ctx.Close()
			return
		}
		fmt.Printf("id: got data (%d) -> writing out result\n", data)
		ctx.Write(data * 2)
		fmt.Println("id: done writing result")
	}
}

var _ pipeline.Processor[int] = &intAdder{}

type intAdder struct {
	sum int
}

func (i *intAdder) Execute(ctx pipeline.ProcessorContext[int]) {
	for {
		data, hasNext := ctx.Read()
		if !hasNext {
			ctx.Write(i.sum)
			ctx.Close()
			return
		}
		i.sum = i.sum + data
	}
}

var _ pipeline.Processor[int] = &errorProcessor{}

type errorProcessor struct {
	err error
}

func (i *errorProcessor) Execute(ctx pipeline.ProcessorContext[int]) {
	ctx.CloseWithError(i.err)
}

var _ pipeline.Processor[int] = &inputFinishedErrorProcessor{}

type inputFinishedErrorProcessor struct {
	err error
}

func (i *inputFinishedErrorProcessor) Execute(ctx pipeline.ProcessorContext[int]) {
	for {
		_, hasNext := ctx.Read()
		if !hasNext {
			ctx.CloseWithError(i.err)
			return
		}
	}
}

var _ pipeline.EventBus = &fakeEventBus{}

type fakeEventBus struct {
	inputChannel  chan pipeline.Event
	outputChannel chan pipeline.Event
}

func (f *fakeEventBus) AutoClosDestination(src pipeline.EventSourceID, dests ...pipeline.EventSourceID) {

}

func (f *fakeEventBus) Connect(eventSource pipeline.EventSource) (<-chan pipeline.Event, chan<- pipeline.Event) {
	f.inputChannel = make(chan pipeline.Event, eventSource.IngressCapacity())
	f.outputChannel = make(chan pipeline.Event)
	return f.inputChannel, f.outputChannel
}

func TestEventProducer_producesSingleOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	eventProducer := NewProducerEventSource[int]("test", newIntProducer([]int{1, 2, 3, 4}), &bus)
	eventProducer.AddOutput("receiver")
	assert.Equal(t, pipeline.EventSourceStateInactive, eventProducer.State())
	eventProducer.Start(ctx)
	assert.Equal(t, pipeline.EventSourceStateActive, eventProducer.State())
	var result []int
	func() {
		for {
			select {
			case e, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := e.(type) {
				case pipeline.DataEvent[int]:
					result = append(result, evt.Data())
				}
			}
		}
	}()
	assert.Equal(t, []int{1, 2, 3, 4}, result)
	assert.Equal(t, pipeline.EventSourceStateSuccess, eventProducer.State())
}

func TestEventProducer_producesMultiOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	outputs := []pipeline.EventSourceID{"r1", "r2", "r3"}
	eventProducer := NewProducerEventSource[int]("test", newIntProducer([]int{1, 2}), &bus)
	eventProducer.AddOutput(outputs...)
	assert.Equal(t, pipeline.EventSourceStateInactive, eventProducer.State())
	eventProducer.Start(ctx)
	assert.Equal(t, pipeline.EventSourceStateActive, eventProducer.State())

	result := map[pipeline.EventSourceID]map[int]pipeline.EventID{}
	func() {
		for {
			select {
			case event, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := event.(type) {
				case pipeline.DataEvent[int]:
					if _, ok := result[evt.Header().Receiver()]; !ok {
						result[evt.Header().Receiver()] = map[int]pipeline.EventID{}
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
	assert.Equal(t, pipeline.EventSourceStateSuccess, eventProducer.State())
}

func TestEventProducer_bailsOnErrorWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	eventProducer := NewProducerEventSource[int]("test", &errorProducer{fmt.Errorf("some error")}, &bus)
	eventProducer.Start(ctx)
	var err error
	for evt, hasNext := <-bus.outputChannel; hasNext; evt, hasNext = <-bus.outputChannel {
		switch evt := evt.(type) {
		case pipeline.ErrorEvent:
			err = evt.Error()
		}
	}
	assert.Equal(t, fmt.Errorf("some error"), err)
	assert.Equal(t, pipeline.EventSourceStateFailed, eventProducer.State())
}

func TestEventProducer_bailsOnErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	eventProducer := NewProducerEventSource[int]("test", newIntProducer([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), &bus)
	eventFactory := event.NewEventFactory[int]("test")
	eventProducer.Start(ctx)
	fmt.Println("blocking on sending error event")
	select {
	case bus.inputChannel <- eventFactory.NewErrorEvent(fmt.Errorf("some error")):
	case <-eventProducer.Done():
		t.Fatal("producer finished before the error event could be generated")
	}
	fmt.Println("error event sent")
	close(bus.inputChannel)
	assert.NotNil(t, eventProducer.Reason())
	assert.Equal(t, fmt.Errorf("some error"), eventProducer.Reason().Error())
	assert.Equal(t, pipeline.EventSourceStateAborted, eventProducer.State())
}

func TestEventConsumer_consumes(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	consumer := &intConsumer{}
	eventConsumer := NewConsumerEventSource[int]("test", consumer, &bus)
	assert.Equal(t, pipeline.EventSourceStateInactive, eventConsumer.State())
	eventConsumer.Start(ctx)
	assert.Equal(t, pipeline.EventSourceStateActive, eventConsumer.State())
	eventFactory := event.NewEventFactory[int]("test")
	for _, value := range []int{1, 2, 3, 4} {
		bus.inputChannel <- eventFactory.NewDataEvent(value)
	}
	close(bus.inputChannel)
	<-eventConsumer.Done()
	assert.Equal(t, []int{1, 2, 3, 4}, consumer.values)
	assert.Equal(t, pipeline.EventSourceStateSuccess, eventConsumer.State())
}

func TestEventConsumer_bailsOnErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	consumer := &intConsumer{}
	eventConsumer := NewConsumerEventSource[int]("test", consumer, &bus)
	eventConsumer.Start(ctx)
	eventFactory := event.NewEventFactory[int]("test")
	bus.inputChannel <- eventFactory.NewErrorEvent(fmt.Errorf("some error"))
	close(bus.inputChannel)
	<-eventConsumer.Done()
	assert.Nil(t, consumer.values)
	assert.NotNil(t, eventConsumer.Reason())
	assert.Equal(t, fmt.Errorf("some error"), eventConsumer.Reason().Error())
	assert.Equal(t, pipeline.EventSourceStateAborted, eventConsumer.State())
}

func TestEventConsumer_bailsOnErrorWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	// ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := fakeEventBus{}
	consumer := &errorConsumer{fmt.Errorf("some error")}
	eventConsumer := NewConsumerEventSource[int]("test", consumer, &bus)
	eventConsumer.Start(ctx)
	// eventFactory := event.NewEventFactory[int]("test")

	errChan := make(chan pipeline.ErrorEvent)
	go func(errChan chan pipeline.ErrorEvent) {
		defer func() {
			close(errChan)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case e, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := e.(type) {
				case pipeline.ErrorEvent:
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
	assert.Equal(t, pipeline.EventSourceStateFailed, eventConsumer.State())
}

func TestEventProcessor_processesSingleOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intDoubler{}
	eventProcessor := NewProcessorEventSource[int]("test", processor, &bus)
	eventProcessor.AddOutput("receiver")
	assert.Equal(t, pipeline.EventSourceStateInactive, eventProcessor.State())
	eventProcessor.Start(ctx)
	assert.Equal(t, pipeline.EventSourceStateActive, eventProcessor.State())
	eventFactory := event.NewEventFactory[int]("producer")

	// produce some input
	go func() {
		for _, value := range []int{1, 2, 3, 4} {
			fmt.Printf("sending %d\n", value)
			bus.inputChannel <- eventFactory.NewDataEvent(value)
		}
		close(bus.inputChannel)
	}()

	// capture output
	var result []int
	for {
		e, hasNext := <-bus.outputChannel
		if !hasNext {
			break
		}
		switch evt := e.(type) {
		case pipeline.DataEvent[int]:
			result = append(result, evt.Data())
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, []int{2, 4, 6, 8}, result)
	assert.Equal(t, pipeline.EventSourceStateSuccess, eventProcessor.State())
}

func TestEventProcessor_processesMultiOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intDoubler{}
	outputs := []pipeline.EventSourceID{"r1", "r2", "r3"}
	eventProcessor := NewProcessorEventSource[int]("test", processor, &bus)
	eventProcessor.AddOutput(outputs...)
	assert.Equal(t, pipeline.EventSourceStateInactive, eventProcessor.State())
	eventProcessor.Start(ctx)
	assert.Equal(t, pipeline.EventSourceStateActive, eventProcessor.State())
	eventFactory := event.NewEventFactory[int]("test")

	// produce some input
	go func() {
		for _, value := range []int{1, 2} {
			bus.inputChannel <- eventFactory.NewDataEvent(value)
		}
		close(bus.inputChannel)
	}()

	result := map[pipeline.EventSourceID]map[int]pipeline.EventID{}
	func() {
		for {
			select {
			case event, hasNext := <-bus.outputChannel:
				if !hasNext {
					return
				}
				switch evt := event.(type) {
				case pipeline.DataEvent[int]:
					if _, ok := result[evt.Header().Receiver()]; !ok {
						result[evt.Header().Receiver()] = map[int]pipeline.EventID{}
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
	assert.Equal(t, pipeline.EventSourceStateSuccess, eventProcessor.State())
}

func TestEventProcessor_callsInputFinishedSingleOutput(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intAdder{}
	eventProcessor := NewProcessorEventSource[int]("test", processor, &bus)
	eventProcessor.AddOutput("receiver")
	eventProcessor.Start(ctx)
	eventFactory := event.NewEventFactory[int]("test")

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
		case pipeline.DataEvent[int]:
			result = append(result, evt.Data())
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, []int{10}, result)
	assert.Equal(t, pipeline.EventSourceStateSuccess, eventProcessor.State())
}

func TestEventProcessor_bailsOnErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &intDoubler{}
	eventProcessor := NewProcessorEventSource[int]("test", processor, &bus)
	eventProcessor.Start(ctx)
	eventFactory := event.NewEventFactory[int]("test")
	bus.inputChannel <- eventFactory.NewErrorEvent(fmt.Errorf("some error"))
	<-eventProcessor.Done()
	assert.NotNil(t, eventProcessor.Reason())
	assert.Equal(t, fmt.Errorf("some error"), eventProcessor.Reason().Error())
	assert.Equal(t, pipeline.EventSourceStateAborted, eventProcessor.State())
}

func TestEventProcessor_bailsOnErrorWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &errorProcessor{fmt.Errorf("some error")}
	eventProcessor := NewProcessorEventSource[int]("test", processor, &bus)
	eventProcessor.Start(ctx)
	eventFactory := event.NewEventFactory[int]("test")

	// produce some input
	go func() {
		bus.inputChannel <- eventFactory.NewDataEvent(1)
		close(bus.inputChannel)
	}()

	// capture output
	var errEvent pipeline.ErrorEvent
	for e, hasNext := <-bus.outputChannel; hasNext; e, hasNext = <-bus.outputChannel {
		switch evt := e.(type) {
		case pipeline.ErrorEvent:
			errEvent = evt
			return
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, fmt.Errorf("some error"), errEvent.Error())
	assert.Equal(t, pipeline.EventSourceStateFailed, eventProcessor.State())
}

func TestEventProcessor_bailsOnErrorOnInputFinishedWithErrorEvent(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()
	bus := fakeEventBus{}
	processor := &inputFinishedErrorProcessor{fmt.Errorf("some error")}
	eventProcessor := NewProcessorEventSource[int]("test", processor, &bus)
	eventProcessor.Start(ctx)
	eventFactory := event.NewEventFactory[int]("test")

	// produce some input
	go func() {
		for _, value := range []int{1, 2, 3, 4} {
			bus.inputChannel <- eventFactory.NewDataEvent(value)
		}
		close(bus.inputChannel)
	}()

	// capture output
	var errEvent pipeline.ErrorEvent
	for event, hasNext := <-bus.outputChannel; hasNext; event, hasNext = <-bus.outputChannel {
		switch evt := event.(type) {
		case pipeline.ErrorEvent:
			errEvent = evt
			return
		}
	}
	<-eventProcessor.Done()
	assert.Equal(t, fmt.Errorf("some error"), errEvent.Error())
	assert.Equal(t, pipeline.EventSourceStateFailed, eventProcessor.State())
}
