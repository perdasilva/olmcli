package uniqueness

import (
	"fmt"

	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ pipeline.Processor[utils.OLMVariable] = &Uniqueness{}

type BundleSet map[string]store.CachedBundle

func (b BundleSet) Entities() []store.CachedBundle {
	var out []store.CachedBundle
	for _, bnd := range b {
		out = append(out, bnd)
	}
	return out
}

type Uniqueness struct{}

func NewUniqueness() *Uniqueness {
	return &Uniqueness{}
}

func (u *Uniqueness) Execute(ctx pipeline.ProcessorContext[utils.OLMVariable]) {
	pkgMap := map[string]BundleSet{}
	gvkMap := map[string]BundleSet{}

	for {
		data, hasNext := ctx.Read()
		if !hasNext {
			var uniquenessVariables = make([]utils.OLMVariable, 0, len(pkgMap)+len(gvkMap))
			for pkgName, bset := range pkgMap {
				entities := bset.Entities()
				utils.Sort(entities, utils.ByChannelAndVersion)
				uniquenessVariables = append(uniquenessVariables, utils.NewUniquenessVariable(pkgUniquenessVariableID(pkgName), entities...))
			}
			for gvk, bset := range gvkMap {
				entities := bset.Entities()
				utils.Sort(entities, utils.ByChannelAndVersion)
				uniquenessVariables = append(uniquenessVariables, utils.NewUniquenessVariable(gvkUniquenessVariableID(gvk), entities...))
			}
			for _, v := range uniquenessVariables {
				ctx.Write(v)
			}
			ctx.Close()
			return
		}

		switch v := data.(type) {
		case *utils.BundleVariable:
			var bundles []store.CachedBundle
			bundles = append(bundles, *v.CachedBundle)
			bundles = append(bundles, v.OrderedEntities()...)
			for _, bundle := range bundles {
				if _, ok := pkgMap[bundle.PackageName]; !ok {
					pkgMap[bundle.PackageName] = BundleSet{}
				}
				pkgMap[bundle.PackageName][bundle.BundleID] = bundle
				for _, gvk := range bundle.ProvidedApis {
					if _, ok := gvkMap[gvk.String()]; !ok {
						gvkMap[gvk.String()] = BundleSet{}
					}
					gvkMap[gvk.String()][bundle.BundleID] = bundle
				}
			}
		}
	}
}

func pkgUniquenessVariableID(packageName string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("package (%s) uniqueness", packageName))
}

func gvkUniquenessVariableID(gvk string) sat.Identifier {
	return sat.Identifier(fmt.Sprintf("gvk (%s) uniqueness", gvk))
}
