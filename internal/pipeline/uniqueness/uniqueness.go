package uniqueness

import (
	"fmt"

	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/eventbus"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ eventbus.Processor[utils.OLMVariable] = &Uniqueness{}

type BundleSet map[string]store.CachedBundle

func (b BundleSet) Entities() []store.CachedBundle {
	var out []store.CachedBundle
	for _, bnd := range b {
		out = append(out, bnd)
	}
	return out
}

type Uniqueness struct {
	pkgMap map[string]BundleSet
	gvkMap map[string]BundleSet
}

func NewUniqueness() *Uniqueness {
	return &Uniqueness{
		pkgMap: map[string]BundleSet{},
		gvkMap: map[string]BundleSet{},
	}
}

func (u *Uniqueness) InputFinished() (<-chan utils.OLMVariable, <-chan error) {
	var uniquenessVariables = make([]utils.OLMVariable, 0, len(u.pkgMap)+len(u.gvkMap))
	for pkgName, bset := range u.pkgMap {
		entities := bset.Entities()
		utils.Sort(entities, utils.ByChannelAndVersion)
		uniquenessVariables = append(uniquenessVariables, utils.NewUniquenessVariable(pkgUniquenessVariableID(pkgName), entities...))
	}
	for gvk, bset := range u.gvkMap {
		entities := bset.Entities()
		utils.Sort(entities, utils.ByChannelAndVersion)
		uniquenessVariables = append(uniquenessVariables, utils.NewUniquenessVariable(gvkUniquenessVariableID(gvk), entities...))
	}

	outputChannel := make(chan utils.OLMVariable)
	errorChannel := make(chan error)

	go func() {
		for _, variable := range uniquenessVariables {
			outputChannel <- variable
		}
		close(outputChannel)
		close(errorChannel)
	}()

	return outputChannel, errorChannel
}

func (u *Uniqueness) Process(data utils.OLMVariable) (*utils.OLMVariable, error) {
	switch v := data.(type) {
	case *utils.BundleVariable:
		var bundles []store.CachedBundle
		bundles = append(bundles, *v.CachedBundle)
		bundles = append(bundles, v.OrderedEntities()...)
		for _, bundle := range bundles {
			if _, ok := u.pkgMap[bundle.PackageName]; !ok {
				u.pkgMap[bundle.PackageName] = BundleSet{}
			}
			u.pkgMap[bundle.PackageName][bundle.BundleID] = bundle
			for _, gvk := range bundle.ProvidedApis {
				if _, ok := u.gvkMap[gvk.String()]; !ok {
					u.gvkMap[gvk.String()] = BundleSet{}
				}
				u.gvkMap[gvk.String()][bundle.BundleID] = bundle
			}
		}
	}
	// fmt.Println("global constraints: processing complete")
	return nil, nil
}

func pkgUniquenessVariableID(packageName string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("package (%s) uniqueness", packageName))
}

func gvkUniquenessVariableID(gvk string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("gvk (%s) uniqueness", gvk))
}
