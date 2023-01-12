package bundle_generator

import (
	"context"
	"sync"

	"github.com/blang/semver/v4"
	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ pipeline.Processor[utils.OLMVariable] = &BundleGenerator{}

type BundleSet map[v2.EntityID]struct{}

type BundleGenerator struct {
	source           *utils.OLMEntitySource
	variables        []utils.OLMVariable
	processedBundles BundleSet
	lock             sync.RWMutex
}

func NewBundleGenerator(source *utils.OLMEntitySource) *BundleGenerator {
	return &BundleGenerator{
		source:           source,
		lock:             sync.RWMutex{},
		processedBundles: BundleSet{},
	}
}

func (b *BundleGenerator) Execute(ctx pipeline.ProcessorContext[utils.OLMVariable]) {
	for {
		data, hasNext := ctx.Read()
		if !hasNext {
			ctx.Close()
			return
		}

		var queue []store.CachedBundle
		queue = append(queue, data.OrderedEntities()...)
		for len(queue) > 0 {
			var head store.CachedBundle
			head, queue = queue[0], queue[1:]
			if _, ok := b.processedBundles[head.ID()]; ok {
				continue
			}
			b.processedBundles[head.ID()] = struct{}{}

			// extract package and gvk dependencies
			var dependencyEntities []store.CachedBundle
			for _, packageDependency := range head.PackageDependencies {
				bundles, err := b.source.GetBundlesForPackage(context.Background(), packageDependency.PackageName, store.InVersionRange(semver.MustParseRange(packageDependency.Version)))
				if err != nil {
					ctx.CloseWithError(err)
					return
				}
				dependencyEntities = append(dependencyEntities, bundles...)
			}

			for _, gvkDependency := range head.RequiredApis {
				bundles, err := b.source.ListBundlesForGVK(context.Background(), gvkDependency.GetGroup(), gvkDependency.GetVersion(), gvkDependency.GetKind())
				if err != nil {
					ctx.CloseWithError(err)
					return
				}
				var bs []store.CachedBundle
				for _, b := range bundles {
					if b.PackageName != head.PackageName {
						bs = append(bs, b)
					}
				}
				dependencyEntities = append(dependencyEntities, bs...)
			}
			utils.Sort(dependencyEntities, utils.ByChannelAndVersionPreferRepository(head.Repository))
			queue = append(queue, dependencyEntities...)
			ctx.Write(utils.NewBundleVariable(&head, dependencyEntities...))
		}
	}
}
