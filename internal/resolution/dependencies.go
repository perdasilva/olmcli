package resolution

import (
	"context"

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

		// extract dependencies
		var dependencyEntities []OLMEntity
		err := source.IterateBundles(ctx, func(bundle *store.CachedBundle) error {
			entity := OLMEntity{bundle}
			if DependencyOf(&head).Keep(&entity) {
				dependencyEntities = append(dependencyEntities, entity)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		Sort(dependencyEntities, ByChannelAndVersion)
		r.queue = append(r.queue, dependencyEntities...)
		variables = append(variables, NewBundleVariable(&head, dependencyEntities...))
	}
	return variables, nil
}
