package bundle_generator

import (
	"context"
	"sync"

	"github.com/blang/semver/v4"
	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/eventbus"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ eventbus.Processor[utils.OLMVariable] = &BundleGenerator{}

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

func (b *BundleGenerator) InputFinished() (<-chan utils.OLMVariable, <-chan error) {
	outputChannel := make(chan utils.OLMVariable)
	errorChannel := make(chan error)
	go func() {
		b.lock.Lock()
		defer b.lock.Unlock()
		var variable utils.OLMVariable
		for len(b.variables) > 0 {
			variable, b.variables = b.variables[0], b.variables[1:]
			outputChannel <- variable
		}
		close(outputChannel)
		close(errorChannel)
	}()
	return outputChannel, errorChannel
}

func (b *BundleGenerator) Process(data utils.OLMVariable) (*utils.OLMVariable, error) {
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
				return nil, err
			}
			dependencyEntities = append(dependencyEntities, bundles...)
		}

		for _, gvkDependency := range head.RequiredApis {
			bundles, err := b.source.ListBundlesForGVK(context.Background(), gvkDependency.GetGroup(), gvkDependency.GetVersion(), gvkDependency.GetKind())
			if err != nil {
				return nil, err
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
		b.variables = append(b.variables, utils.NewBundleVariable(&head, dependencyEntities...))
	}
	var variable utils.OLMVariable
	variable, b.variables = b.variables[0], b.variables[1:]
	return &variable, nil
}
