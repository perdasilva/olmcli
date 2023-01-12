package solver

import (
	"context"

	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ pipeline.Processor[utils.OLMVariable] = &Solver{}

type Solver struct {
}

func NewSolver() *Solver {
	return &Solver{}
}

func (s *Solver) Execute(ctx pipeline.ProcessorContext[utils.OLMVariable]) {
	var variables []utils.OLMVariable
	for {
		data, hasNext := ctx.Read()
		if !hasNext {
			// resolve
			solver, err := sat.NewSolver(sat.WithGenericIntput(variables))
			if err != nil {
				ctx.CloseWithError(err)
				return
			}
			solution, err := solver.Solve(context.Background())
			for _, variable := range solution {
				switch v := variable.(type) {
				case utils.OLMVariable:
					ctx.Write(v)
				}
			}
			ctx.Close()
			return
		}
		variables = append(variables, data)
	}
}
