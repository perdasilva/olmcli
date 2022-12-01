package constraints

import (
	"context"

	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/resolution/filter"
	"github.com/perdasilva/olmcli/internal/resolution/properties"
	"github.com/perdasilva/olmcli/internal/resolution/sort"
	"github.com/tidwall/gjson"
)

type EntityConstraints struct{}

func (d *EntityConstraints) GetVariables(ctx context.Context, querier entitysource.EntityQuerier) ([]sat.Variable, error) {
	variables := make([]sat.Variable, 0)
	err := querier.Iterate(ctx, func(entity *entitysource.Entity) error {
		solverVar := &OLMVariable{
			id: sat.Identifier(entity.ID()),
		}

		// collect package dependencies
		if pkgRequirementsJson, err := entity.GetProperty(properties.OLMPackageRequired); err == nil {
			pkgRequirements := gjson.Parse(pkgRequirementsJson).Array()
			for _, pkgReqs := range pkgRequirements {
				packageName := pkgReqs.Get("packageName").String()
				versionRange := pkgReqs.Get("versionRange").String()
				resultSet, err := querier.Filter(ctx, entitysource.And(filter.WithPackageName(packageName), filter.WithinVersion(versionRange)))
				if err != nil {
					return err
				}
				resultSet = resultSet.Sort(sort.ByChannelAndVersion)
				solverVar.constraints = append(solverVar.constraints, sat.Dependency(toSolverIdentifier(resultSet.CollectIds())...))
			}
		}

		// collect gvk dependencies
		if gvkRequirementsJson, err := entity.GetProperty(properties.OLMGVKRequired); err == nil {
			gvkRequirements := gjson.Parse(gvkRequirementsJson).Array()
			for _, gvkReqs := range gvkRequirements {
				group := gvkReqs.Get("group").String()
				version := gvkReqs.Get("version").String()
				kind := gvkReqs.Get("kind").String()

				resultSet, err := querier.Filter(ctx, entitysource.And(filter.WithExportsGVK(group, version, kind)))
				if err != nil {
					return err
				}
				resultSet = resultSet.Sort(sort.ByChannelAndVersion)
				solverVar.constraints = append(solverVar.constraints, sat.Dependency(toSolverIdentifier(resultSet.CollectIds())...))
			}
		}
		variables = append(variables, solverVar)
		return nil
	})
	return variables, err
}
