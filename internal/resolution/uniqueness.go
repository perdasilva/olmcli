package resolution

import (
	"context"
	"fmt"

	"github.com/operator-framework/deppy/pkg/sat"
	v2 "github.com/operator-framework/deppy/pkg/v2"
)

type uniqueness struct{}

func NewUniquenessVariableSource() v2.VariableSource[OLMEntity, OLMVariable, IterableOLMEntitySource] {
	return &uniqueness{}
}

func (r *uniqueness) GetVariables(ctx context.Context, source IterableOLMEntitySource) ([]OLMVariable, error) {
	pkgMap := map[string][]OLMEntity{}
	gvkMap := map[string][]OLMEntity{}

	err := source.Iterate(ctx, func(entity *OLMEntity) error {
		pkgMap[entity.PackageName] = append(pkgMap[entity.PackageName], *entity)
		for _, gvk := range entity.ProvidedApis {
			gvkMap[gvk.String()] = append(gvkMap[gvk.String()], *entity)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var uniquenessVariables = make([]OLMVariable, 0, len(pkgMap)+len(gvkMap))
	for pkgName, entities := range pkgMap {
		Sort(entities, ByChannelAndVersion)
		uniquenessVariables = append(uniquenessVariables, NewUniquenessVariable(pkgUniquenessVariableID(pkgName), entities...))
	}
	for gvk, entities := range gvkMap {
		Sort(entities, ByChannelAndVersion)
		uniquenessVariables = append(uniquenessVariables, NewUniquenessVariable(gvkUniquenessVariableID(gvk), entities...))
	}
	return uniquenessVariables, nil
}

func pkgUniquenessVariableID(packageName string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("package (%s) uniqueness", packageName))
}

func gvkUniquenessVariableID(gvk string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("gvk (%s) uniqueness", gvk))
}
