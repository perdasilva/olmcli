package resolution

import (
	"context"

	"github.com/blang/semver/v4"
	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
)

var _ v2.VariableSource[OLMEntity, OLMVariable, *OLMEntitySource] = &DependenciesVariableSource{}

type DependenciesVariableSource struct {
	queue []OLMEntity
}

func NewDependenciesVariableSource(seedEntities ...OLMEntity) *DependenciesVariableSource {
	return &DependenciesVariableSource{
		queue: seedEntities,
	}
}

func (r *DependenciesVariableSource) GetVariables(ctx context.Context, source *OLMEntitySource) ([]OLMVariable, error) {
	processedEntities := map[v2.EntityID]struct{}{}
	var variables []OLMVariable

	for len(r.queue) > 0 {
		var head OLMEntity
		head, r.queue = r.queue[0], r.queue[1:]
		if _, ok := processedEntities[head.ID()]; ok {
			continue
		}
		processedEntities[head.ID()] = struct{}{}

		// extract package and gvk dependencies
		var dependencyEntities []OLMEntity
		for _, packageDependency := range head.PackageDependencies {
			bundles, err := source.GetBundlesForPackage(ctx, packageDependency.PackageName, store.InVersionRange(semver.MustParseRange(packageDependency.Version)))
			if err != nil {
				return nil, err
			}
			for _, bundle := range bundles {
				dependencyEntities = append(dependencyEntities, OLMEntity{&bundle})
			}
		}

		for _, gvkDependency := range head.RequiredApis {
			bundles, err := source.ListBundlesForGVK(ctx, gvkDependency.GetGroup(), gvkDependency.GetVersion(), gvkDependency.GetKind())
			if err != nil {
				return nil, err
			}
			for _, bundle := range bundles {
				dependencyEntities = append(dependencyEntities, OLMEntity{&bundle})
			}
		}
		Sort(dependencyEntities, ByChannelAndVersion)
		r.queue = append(r.queue, dependencyEntities...)
		variables = append(variables, NewBundleVariable(&head, dependencyEntities...))
	}
	return variables, nil
}
