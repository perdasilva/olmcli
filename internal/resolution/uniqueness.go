package resolution

import (
	"context"
	"fmt"

	"github.com/operator-framework/deppy/pkg/sat"
	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

type uniqueness struct{}

func NewUniquenessVariableSource() v2.VariableSource[*store.CachedBundle, utils.OLMVariable, utils.IterableOLMEntitySource] {
	return &uniqueness{}
}

func (r *uniqueness) GetVariables(ctx context.Context, source utils.IterableOLMEntitySource) ([]utils.OLMVariable, error) {
	pkgMap := map[string][]store.CachedBundle{}
	gvkMap := map[string][]store.CachedBundle{}

	err := source.Iterate(ctx, func(entity *store.CachedBundle) error {
		pkgMap[entity.PackageName] = append(pkgMap[entity.PackageName], *entity)
		for _, gvk := range entity.ProvidedApis {
			gvkMap[gvk.String()] = append(gvkMap[gvk.String()], *entity)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var uniquenessVariables = make([]utils.OLMVariable, 0, len(pkgMap)+len(gvkMap))
	for pkgName, entities := range pkgMap {
		utils.Sort(entities, utils.ByChannelAndVersion)
		uniquenessVariables = append(uniquenessVariables, utils.NewUniquenessVariable(pkgUniquenessVariableID(pkgName), entities...))
	}
	for gvk, entities := range gvkMap {
		utils.Sort(entities, utils.ByChannelAndVersion)
		uniquenessVariables = append(uniquenessVariables, utils.NewUniquenessVariable(gvkUniquenessVariableID(gvk), entities...))
	}
	return uniquenessVariables, nil
}

func pkgUniquenessVariableID(packageName string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("package (%s) uniqueness", packageName))
}

func gvkUniquenessVariableID(gvk string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("gvk (%s) uniqueness", gvk))
}
