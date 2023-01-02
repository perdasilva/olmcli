package resolution

import (
	"context"

	"github.com/blang/semver/v4"
	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ v2.VariableSource[*store.CachedBundle, utils.OLMVariable, *utils.OLMEntitySource] = &DependenciesVariableSource{}

type DependenciesVariableSource struct {
	queue []store.CachedBundle
}

func NewBundleVariableSource(seedEntities ...store.CachedBundle) *DependenciesVariableSource {
	return &DependenciesVariableSource{
		queue: seedEntities,
	}
}

func (r *DependenciesVariableSource) GetVariables(ctx context.Context, source *utils.OLMEntitySource) ([]utils.OLMVariable, error) {
	processedEntities := map[v2.EntityID]struct{}{}
	var variables []utils.OLMVariable

	for len(r.queue) > 0 {
		var head store.CachedBundle
		head, r.queue = r.queue[0], r.queue[1:]
		if _, ok := processedEntities[head.ID()]; ok {
			continue
		}
		processedEntities[head.ID()] = struct{}{}

		// extract package and gvk dependencies
		var dependencyEntities []store.CachedBundle
		for _, packageDependency := range head.PackageDependencies {
			bundles, err := source.GetBundlesForPackage(ctx, packageDependency.PackageName, store.InVersionRange(semver.MustParseRange(packageDependency.Version)))
			if err != nil {
				return nil, err
			}
			dependencyEntities = append(dependencyEntities, bundles...)
		}

		for _, gvkDependency := range head.RequiredApis {
			bundles, err := source.ListBundlesForGVK(ctx, gvkDependency.GetGroup(), gvkDependency.GetVersion(), gvkDependency.GetKind())
			if err != nil {
				return nil, err
			}
			dependencyEntities = append(dependencyEntities, bundles...)
		}
		utils.Sort(dependencyEntities, utils.ByChannelAndVersionPreferRepository(head.Repository))
		r.queue = append(r.queue, dependencyEntities...)
		variables = append(variables, utils.NewBundleVariable(&head, dependencyEntities...))
	}
	return variables, nil
}
