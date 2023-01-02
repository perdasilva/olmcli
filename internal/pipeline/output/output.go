package output

import (
	"sync"

	"github.com/perdasilva/olmcli/internal/eventbus"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ eventbus.Consumer[utils.OLMVariable] = &Output{}

type Output struct {
	variables []utils.OLMVariable
	lock      sync.RWMutex
}

func NewOutput() *Output {
	return &Output{
		lock: sync.RWMutex{},
	}
}

func (o *Output) Consume(data utils.OLMVariable) error {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.variables = append(o.variables, data)
	return nil
}

func (o *Output) Variables() []utils.OLMVariable {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return o.variables
}
