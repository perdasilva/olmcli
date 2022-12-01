package constraints

import (
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/deppy/pkg/sat"
)

func toSolverIdentifier(ids []entitysource.EntityID) []sat.Identifier {
	out := make([]sat.Identifier, len(ids))
	for i, _ := range ids {
		out[i] = sat.Identifier(ids[i])
	}
	return out
}
