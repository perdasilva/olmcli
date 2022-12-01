package filter

import (
	"github.com/blang/semver/v4"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/perdasilva/olmcli/internal/resolution/properties"
	"github.com/tidwall/gjson"
)

func WithPackageName(packageName string) entitysource.Predicate {
	return func(entity *entitysource.Entity) bool {
		if pkgName, err := entity.GetProperty(properties.OLMPackageName); err == nil {
			return pkgName == packageName
		}
		return false
	}
}

func WithinVersion(semverRange string) entitysource.Predicate {
	return func(entity *entitysource.Entity) bool {
		if v, err := entity.GetProperty(properties.OLMVersion); err == nil {
			vrange := semver.MustParseRange(semverRange)
			version := semver.MustParse(v)
			return vrange(version)
		}
		return false
	}
}

func WithChannel(channel string) entitysource.Predicate {
	return func(entity *entitysource.Entity) bool {
		if channel == "" {
			return true
		}
		if c, err := entity.GetProperty(properties.OLMChannel); err == nil {
			return c == channel
		}
		return false
	}
}

func WithExportsGVK(group string, version string, kind string) entitysource.Predicate {
	return func(entity *entitysource.Entity) bool {
		if g, err := entity.GetProperty(properties.OLMGVK); err == nil {
			for _, gvk := range gjson.Parse(g).Array() {
				if gjson.Get(gvk.String(), "group").String() == group && gjson.Get(gvk.String(), "version").String() == version && gjson.Get(gvk.String(), "kind").String() == kind {
					return true
				}
			}
		}
		return false
	}
}
