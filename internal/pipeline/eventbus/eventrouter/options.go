package eventrouter

import (
	"github.com/perdasilva/olmcli/internal/pipeline"
)

type Option func(router *Router)

func WithDebugChannel(debugChannel chan<- pipeline.Event) Option {
	return func(router *Router) {
		router.debugChannel = debugChannel
	}
}
