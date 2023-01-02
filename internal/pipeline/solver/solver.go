package solver

import (
	"context"

	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/eventbus"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ eventbus.Processor[utils.OLMVariable] = &Solver{}

type Solver struct {
	variables []utils.OLMVariable
}

func NewSolver() *Solver {
	return &Solver{}
}

func (s *Solver) InputFinished() (<-chan utils.OLMVariable, <-chan error) {
	outputChannel := make(chan utils.OLMVariable)
	errorChannel := make(chan error)

	go func() {
		defer func() {
			close(outputChannel)
			close(errorChannel)
		}()
		solver, err := sat.NewSolver(sat.WithGenericIntput(s.variables))
		if err != nil {
			errorChannel <- err
			return
		}
		solution, err := solver.Solve(context.Background())
		if err != nil {
			errorChannel <- err
			return
		}
		for _, variable := range solution {
			switch v := variable.(type) {
			case utils.OLMVariable:
				outputChannel <- v
			}
		}
	}()

	return outputChannel, errorChannel
}

func (s *Solver) Process(data utils.OLMVariable) (*utils.OLMVariable, error) {
	s.variables = append(s.variables, data)
	return nil, nil
}
