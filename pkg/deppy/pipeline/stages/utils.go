package stages

import "github.com/perdasilva/olmcli/pkg/deppy"

var _ deppy.ResolutionProblem = &ResolutionProblem{}

// ResolutionProblem represents a problem that can be solved by the deppy solver
type ResolutionProblem struct {
	id      deppy.Identifier
	problem []deppy.Variable
	options []deppy.ResolutionOption
}

func NewResolutionProblem(id deppy.Identifier, problem []deppy.Variable) ResolutionProblem {
	return ResolutionProblem{
		id:      id,
		problem: problem,
	}
}

func (r ResolutionProblem) ResolutionProblemID() deppy.Identifier {
	return r.id
}

func (r ResolutionProblem) GetVariables() ([]deppy.Variable, error) {
	return r.problem, nil
}

func (r ResolutionProblem) Options() []deppy.ResolutionOption {
	return r.options
}
