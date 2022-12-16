package resolution

import (
	"sort"

	"github.com/blang/semver/v4"
	"github.com/perdasilva/olmcli/internal/store"
)

type Comparable[E any] func(e1 *E, e2 *E) bool

func Sort[E any](slice []E, fn Comparable[E]) {
	sort.SliceStable(slice, func(i, j int) bool {
		return fn(&slice[i], &slice[j])
	})
}

var _ Comparable[store.CachedBundle] = ByChannelAndVersion

func ByChannelAndVersionPreferRepository(repositoryID string) Comparable[store.CachedBundle] {
	return func(e1 *store.CachedBundle, e2 *store.CachedBundle) bool {
		if e1.Repository != e2.Repository {
			if e1.Repository == repositoryID {
				return true
			}
			if e2.Repository == repositoryID {
				return false
			}
		}
		return ByChannelAndVersion(e1, e2)
	}
}

func ByChannelAndVersion(e1 *store.CachedBundle, e2 *store.CachedBundle) bool {
	if e1.Repository != e2.Repository {
		return e1.Repository < e2.Repository
	}

	if e1.PackageName != e2.PackageName {
		return e1.PackageName < e2.PackageName
	}

	if e1.ChannelName != e2.ChannelName {
		if e1.ChannelName == e1.DefaultChannelName || e2.ChannelName == e2.DefaultChannelName {
			return e1.ChannelName == e1.DefaultChannelName
		}
		return e1.ChannelName < e2.ChannelName
	}

	return semver.MustParse(e1.Version).GT(semver.MustParse(e2.Version))
}

var _ Comparable[store.CachedBundle] = ByVersionIncreasing

func ByVersionIncreasing(e1 *store.CachedBundle, e2 *store.CachedBundle) bool {
	return semver.MustParse(e1.Version).LT(semver.MustParse(e2.Version))
}
