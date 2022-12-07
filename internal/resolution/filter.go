package resolution

import (
	"encoding/json"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/perdasilva/olmcli/internal/store"
)

type Predicate[E any] interface {
	Keep(*E) bool
}

type and[E any] struct {
	predicates []Predicate[E]
}

func (a *and[E]) Keep(entity *E) bool {
	for _, predicate := range a.predicates {
		if !(predicate.Keep(entity)) {
			return false
		}
	}
	return true
}

func And[E any](predicates ...Predicate[E]) Predicate[E] {
	return &and[E]{
		predicates: predicates,
	}
}

var _ Predicate[store.CachedBundle] = &inRepository{}

type inRepository struct {
	repositoryName string
}

func (r *inRepository) Keep(bundle *store.CachedBundle) bool {
	if bundle == nil {
		return false
	}
	return bundle.Repository == r.repositoryName
}

func InRepository(repositoryName string) Predicate[store.CachedBundle] {
	return &inRepository{
		repositoryName: repositoryName,
	}
}

var _ Predicate[store.CachedBundle] = &inPackage{}

type inPackage struct {
	packageName string
}

func (p *inPackage) Keep(bundle *store.CachedBundle) bool {
	if bundle == nil {
		return false
	}
	return bundle.PackageName == p.packageName
}

func InPackage(packageName string) Predicate[store.CachedBundle] {
	return &inPackage{
		packageName: packageName,
	}
}

var _ Predicate[store.CachedBundle] = &inChannel{}

type inChannel struct {
	channelName string
}

func (c *inChannel) Keep(bundle *store.CachedBundle) bool {
	if bundle == nil {
		return false
	}
	return bundle.ChannelName == c.channelName
}

func InChannel(channelName string) Predicate[store.CachedBundle] {
	return &inChannel{
		channelName: channelName,
	}
}

var _ Predicate[store.CachedBundle] = &inSemverRange{}

type inSemverRange struct {
	versionRange semver.Range
}

func InSemverRange(versionRange semver.Range) Predicate[store.CachedBundle] {
	return &inSemverRange{
		versionRange: versionRange,
	}
}

func (v *inSemverRange) Keep(bundle *store.CachedBundle) bool {
	if bundle == nil {
		return false
	}

	version, err := semver.Parse(bundle.Version)
	if err != nil {
		return false
	}

	return v.versionRange(version)
}

var _ Predicate[store.CachedBundle] = &dependencyOf{}

type dependencyOf struct {
	entity *store.CachedBundle
}

func DependencyOf(entity *store.CachedBundle) Predicate[store.CachedBundle] {
	return &dependencyOf{
		entity: entity,
	}
}

func (v *dependencyOf) Keep(bundle *store.CachedBundle) bool {
	if bundle == nil {
		return false
	}

	for _, requiredAPI := range v.entity.GetRequiredApis() {
		for _, providedAPI := range bundle.ProvidedApis {
			if providedAPI.String() == requiredAPI.String() {
				return true
			}
		}
	}

	for _, dependency := range v.entity.GetDependencies() {
		switch dependency.GetType() {
		case property.TypePackage:
			requiredPackage := &struct {
				PackageName  string `json:"packageName"`
				VersionRange string `json:"version"`
			}{}
			if err := json.Unmarshal([]byte(dependency.GetValue()), requiredPackage); err == nil {
				versionRange, err := semver.ParseRange(requiredPackage.VersionRange)
				if err == nil {
					if And(InPackage(requiredPackage.PackageName), InSemverRange(versionRange)).Keep(bundle) {
						return true
					}
				}
			}
		}
	}
	return false
}
