package sort

import (
	"github.com/blang/semver/v4"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/perdasilva/olmcli/internal/resolution/properties"
)

func ByChannelAndVersion(e1 *entitysource.Entity, e2 *entitysource.Entity) bool {
	e1PackageName, err := e1.GetProperty(properties.OLMPackageName)
	if err != nil {
		return false
	}

	e2PackageName, err := e2.GetProperty(properties.OLMPackageName)
	if err != nil {
		return false
	}

	if e1PackageName != e2PackageName {
		return e1PackageName < e2PackageName
	}

	e1Channel, err := e1.GetProperty(properties.OLMChannel)
	if err != nil {
		return false
	}
	e2Channel, err := e2.GetProperty(properties.OLMChannel)
	if err != nil {
		return false
	}

	if e1Channel != e2Channel {
		e1DefaultChannel, err := e1.GetProperty(properties.OLMDefaultChannel)
		if err != nil {
			return false
		}
		e2DefaultChannel, err := e2.GetProperty(properties.OLMDefaultChannel)
		if err != nil {
			return false
		}

		if e1Channel == e1DefaultChannel {
			return true
		}
		if e2Channel == e2DefaultChannel {
			return false
		}
		return e1Channel < e2Channel
	}

	v1, err := e1.GetProperty(properties.OLMVersion)
	if err != nil {
		return false
	}
	v2, err := e2.GetProperty(properties.OLMVersion)
	if err != nil {
		return false
	}

	return semver.MustParse(v1).GT(semver.MustParse(v2))
}

func ByVersionIncreasing(e1 *entitysource.Entity, e2 *entitysource.Entity) bool {
	v1, err := e1.GetProperty(properties.OLMVersion)
	if err != nil {
		return false
	}

	v2, err := e1.GetProperty(properties.OLMVersion)
	if err != nil {
		return false
	}
	return semver.MustParse(v1).LT(semver.MustParse(v2))
}
