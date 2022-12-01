package constraints

import (
	"context"
	"fmt"

	"github.com/operator-framework/deppy/pkg/constraints"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/resolution/properties"
	"github.com/perdasilva/olmcli/internal/resolution/sort"
)

var _ constraints.ConstraintGenerator = &PkgUniqueness{}

type PkgUniqueness struct{}

func (p *PkgUniqueness) GetVariables(ctx context.Context, querier entitysource.EntityQuerier) ([]sat.Variable, error) {
	resultMap, err := querier.GroupBy(ctx, func(e1 *entitysource.Entity) []string {
		packageName, err := e1.GetProperty(properties.OLMPackageName)
		if err != nil {
			return nil
		}
		return []string{packageName}
	})
	if err != nil {
		return nil, err
	}
	resultMap = resultMap.Sort(sort.ByVersionIncreasing)

	vars := make([]sat.Variable, 0, len(resultMap))
	for key, entities := range resultMap {
		v := &OLMVariable{
			id: sat.Identifier(fmt.Sprintf("%s uniqueness", key)),
			constraints: []sat.Constraint{
				sat.AtMost(1, toSolverIdentifier(entities.CollectIds())...),
			},
		}
		vars = append(vars, v)
	}

	return vars, nil
}
