package constraints

import "github.com/operator-framework/deppy/pkg/sat"

var _ sat.Variable = &OLMVariable{}

type OLMVariable struct {
	id          sat.Identifier
	constraints []sat.Constraint
	isRoot      bool
}

func (o *OLMVariable) Identifier() sat.Identifier {
	return o.id
}

func (o *OLMVariable) Constraints() []sat.Constraint {
	return o.constraints
}

func (o *OLMVariable) IsRoot() bool {
	return o.isRoot
}
