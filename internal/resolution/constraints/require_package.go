package constraints

import (
	"context"
	"fmt"

	"github.com/operator-framework/deppy/pkg/constraints"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/resolution/filter"
	"github.com/perdasilva/olmcli/internal/resolution/sort"
)

var _ constraints.ConstraintGenerator = &RequirePackage{}

type RequirePackage struct {
	PackageName  string
	VersionRange string
	Channel      string
}

func (r *RequirePackage) GetVariables(ctx context.Context, querier entitysource.EntityQuerier) ([]sat.Variable, error) {
	vars := make([]sat.Variable, 0)
	resultSet, err := querier.Filter(ctx, entitysource.And(filter.WithPackageName(r.PackageName), filter.WithinVersion(r.VersionRange), filter.WithChannel(r.Channel)))
	if err != nil {
		return nil, err
	}
	entities := resultSet.Sort(sort.ByChannelAndVersion)
	vars = append(vars, &OLMVariable{
		id: sat.Identifier(fmt.Sprintf("package %s required at %s", r.PackageName, r.VersionRange)),
		constraints: []sat.Constraint{
			sat.Mandatory(),
			sat.Dependency(toSolverIdentifier(entities.CollectIds())...),
		},
		isRoot: true,
	})
	return vars, nil
}
