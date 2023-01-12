package output

import (
	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ pipeline.Consumer[utils.OLMVariable] = &Output{}

type Output struct {
	variables []utils.OLMVariable
}

func NewOutput() *Output {
	return &Output{}
}

func (o *Output) Execute(ctx pipeline.ConsumerContext[utils.OLMVariable]) {
	for {
		data, hasNext := ctx.Read()
		if !hasNext {
			ctx.Close()
			return
		}
		o.variables = append(o.variables, data)
	}
}

func (o *Output) Variables() []utils.OLMVariable {
	return o.variables
}
