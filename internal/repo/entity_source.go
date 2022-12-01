package repo

import (
	"context"
	"fmt"
	"strings"

	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/tidwall/gjson"
)

var _ entitysource.EntitySource = &PackageDatabaseEntitySource{}

type PackageDatabaseEntitySource struct {
	packageDatabase PackageDatabase
}

func NewPackageDatabaseEntitySource(packageDatabase PackageDatabase) *PackageDatabaseEntitySource {
	return &PackageDatabaseEntitySource{
		packageDatabase: packageDatabase,
	}
}

func (b *PackageDatabaseEntitySource) Get(_ context.Context, id entitysource.EntityID) *entitysource.Entity {
	entity, _ := b.entityFromBundleId(string(id))
	return entity
}

func (b *PackageDatabaseEntitySource) Filter(_ context.Context, filter entitysource.Predicate) (entitysource.EntityList, error) {
	result := entitysource.EntityList{}
	err := b.packageDatabase.IterateBundles(func(bundle *CachedBundle) error {
		entity, err := b.bundleToDeppyEntity(bundle)
		if err != nil {
			return err
		}
		if filter(entity) {
			result = append(result, *entity)
		}
		return nil
	})
	return result, err
}

func (b *PackageDatabaseEntitySource) GroupBy(_ context.Context, fn entitysource.GroupByFunction) (entitysource.EntityListMap, error) {
	result := entitysource.EntityListMap{}
	err := b.packageDatabase.IterateBundles(func(bundle *CachedBundle) error {
		entity, err := b.bundleToDeppyEntity(bundle)
		if err != nil {
			return err
		}
		keys := fn(entity)
		for _, key := range keys {
			result[key] = append(result[key], *entity)
		}
		return nil
	})
	return result, err
}

func (b *PackageDatabaseEntitySource) Iterate(_ context.Context, fn entitysource.IteratorFunction) error {
	return b.packageDatabase.IterateBundles(func(bundle *CachedBundle) error {
		entity, err := b.bundleToDeppyEntity(bundle)
		if err != nil {
			return err
		}
		return fn(entity)
	})
}

func (b *PackageDatabaseEntitySource) GetContent(_ context.Context, id entitysource.EntityID) (interface{}, error) {
	return nil, nil
}

func (b *PackageDatabaseEntitySource) entityFromBundleId(bundleID string) (*entitysource.Entity, error) {
	bundle, err := b.packageDatabase.GetBundle(bundleID)
	if err != nil {
		return nil, err
	}
	if bundle == nil {
		return nil, fmt.Errorf("bundle (%s) not found", bundleID)
	}
	packageID := GetPackageKey(bundle.Repository, bundle.PackageName)
	pkg, err := b.packageDatabase.GetPackage(packageID)
	if err != nil {
		return nil, err
	}
	if pkg == nil {
		return nil, fmt.Errorf("package (%s) not found", packageID)
	}
	return b.bundleToDeppyEntity(bundle)
}

func (b *PackageDatabaseEntitySource) bundleToDeppyEntity(bundle *CachedBundle) (*entitysource.Entity, error) {

	entityId := entitysource.EntityID(bundle.ID)
	properties := map[string]string{}
	for _, prop := range bundle.Properties {
		switch prop.Type {
		case property.TypePackage:
			properties["olm.packageName"] = gjson.Get(prop.Value, "packageName").String()
			properties["olm.version"] = gjson.Get(prop.Value, "version").String()
		default:
			if curValue, ok := properties[prop.Type]; ok {
				if curValue[0] != '[' {
					curValue = "[" + curValue + "]"
				}
				properties[prop.Type] = curValue[0:len(curValue)-1] + "," + prop.Value + "]"
			} else {
				properties[prop.Type] = prop.Value
			}
		}
	}
	properties["olm.channel"] = bundle.ChannelName
	properties["olm.defaultChannel"] = bundle.DefaultChannelName

	if bundle.Replaces != "" {
		properties["olm.replaces"] = bundle.Replaces
	}

	if bundle.SkipRange != "" {
		properties["olm.skipRange"] = bundle.SkipRange
	}

	if len(bundle.Skips) > 0 {
		properties["olm.skips"] = fmt.Sprintf("[%s]", strings.Join(bundle.Skips, ","))
	}

	return entitysource.NewEntity(entityId, properties), nil
}
