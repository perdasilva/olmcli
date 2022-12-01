package constraints

import (
	"context"
	"fmt"

	"github.com/operator-framework/deppy/pkg/constraints"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/resolution/properties"
	"github.com/perdasilva/olmcli/internal/resolution/sort"
	"github.com/tidwall/gjson"
)

var _ constraints.ConstraintGenerator = &GVKUniqueness{}

type GVKUniqueness struct{}

func (g *GVKUniqueness) GetVariables(ctx context.Context, querier entitysource.EntityQuerier) ([]sat.Variable, error) {
	resultMap, err := querier.GroupBy(ctx, func(e1 *entitysource.Entity) []string {
		gvks, err := e1.GetProperty(properties.OLMGVK)
		if err != nil {
			return nil
		}
		gvkArray := gjson.Parse(gvks).Array()
		out := make([]string, 0)
		for _, val := range gvkArray {
			out = append(out, fmt.Sprintf("%s/%s/%s", val.Get("group"), val.Get("version"), val.Get("kind")))
		}
		return out
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
