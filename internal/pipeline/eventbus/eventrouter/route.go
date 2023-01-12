package eventrouter

import (
	"context"
	"sync"

	"github.com/perdasilva/olmcli/internal/pipeline"
)

// Route tracks the input and output channels for an event source
type Route struct {
	eventSourceID pipeline.EventSourceID
	// channel for events processed by the event source (input)
	inputChannel chan pipeline.Event

	// channel for events produced by the event source (output)
	outputChannel chan pipeline.Event

	// whether the input channel has been closed, can happen when:
	// 1. the event source has closed its output channel (signalling end-of-output/processing)
	// 2. closed by an auto-close because all input sources into this event source have closed their output channels (end-of-input)
	inputChannelClosed bool

	// both input and output channels are closed
	connectionDoneListeners map[chan<- struct{}]struct{}

	lock sync.RWMutex
}

func (c *Route) NotifyOnConnectionDone(doneCh chan<- struct{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connectionDoneListeners[doneCh] = struct{}{}
}

func (c *Route) CloseInputChannel() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.inputChannelClosed && c.inputChannel != nil {
		close(c.inputChannel)
		c.inputChannelClosed = true
	}
}

func (c *Route) InputChannel() <-chan pipeline.Event {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.inputChannel
}

func (c *Route) OutputChannel() chan<- pipeline.Event {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.outputChannel
}

func (c *Route) notifyConnectionDoneListeners(ctx context.Context) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for doneCh, _ := range c.connectionDoneListeners {
		go func(doneCh chan<- struct{}) {
			select {
			case <-ctx.Done():
				return
			case doneCh <- struct{}{}:
				return
			}
		}(doneCh)
	}
}

func (c *Route) ConnectionDone(ctx context.Context) {
	c.CloseInputChannel()
	c.notifyConnectionDoneListeners(ctx)
}
