package resolution

import (
	"context"

	v2 "github.com/operator-framework/deppy/pkg/v2"
)

var _ v2.VariableSource[OLMEntity, OLMVariable, *OLMEntitySource] = &olmVariableSource{}

type olmVariableSource struct {
	requiredPackages []RequiredPackage
}

func OLMVariableSource(requiredPackages []RequiredPackage) (v2.VariableSource[OLMEntity, OLMVariable, *OLMEntitySource], error) {
	olmVariableSource := &olmVariableSource{
		requiredPackages: requiredPackages,
	}
	return olmVariableSource, nil
}

func (r *olmVariableSource) GetVariables(ctx context.Context, source *OLMEntitySource) ([]OLMVariable, error) {
	var variables []OLMVariable
	entitySet := OLMEntitySet{}

	// collect all required package variables
	for _, reqPkg := range r.requiredPackages {
		reqPkgVars, err := reqPkg.GetVariables(ctx, source)
		if err != nil {
			return nil, err
		}
		variables = append(variables, reqPkgVars...)
		for _, reqPkgVar := range reqPkgVars {
			for _, entity := range reqPkgVar.OrderedEntities() {
				if _, ok := entitySet[entity.ID()]; !ok {
					entitySet[entity.ID()] = entity
				}
			}
		}
	}

	// collect bundles and dependencies
	entities := make([]OLMEntity, 0, len(entitySet))
	for _, entity := range entitySet {
		entities = append(entities, entity)
	}
	dependencyVariableSource := NewDependenciesVariableSource(entities...)
	bundleVariables, err := dependencyVariableSource.GetVariables(ctx, source)
	if err != nil {
		return nil, err
	}
	variables = append(variables, bundleVariables...)
	for _, v := range bundleVariables {
		for _, entity := range v.OrderedEntities() {
			if _, ok := entitySet[entity.ID()]; !ok {
				entitySet[entity.ID()] = entity
			}
		}
	}

	// collect uniqueness variables
	uniquenessVariableSource := NewUniquenessVariableSource()
	uniquenessVariables, err := uniquenessVariableSource.GetVariables(ctx, NewIterableEntitySource("packageSet", entitySet))
	if err != nil {
		return nil, err
	}
	variables = append(variables, uniquenessVariables...)
	return variables, nil
}
