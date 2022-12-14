package resolution

import (
	"context"
	"time"

	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/sirupsen/logrus"
)

var _ v2.VariableSource[*store.CachedBundle, OLMVariable, *OLMEntitySource] = &olmVariableSource{}

type olmVariableSource struct {
	requiredPackages []*RequiredPackage
	logger           *logrus.Logger
}

func OLMVariableSource(requiredPackages []*RequiredPackage, logger *logrus.Logger) (v2.VariableSource[*store.CachedBundle, OLMVariable, *OLMEntitySource], error) {
	olmVariableSource := &olmVariableSource{
		requiredPackages: requiredPackages,
		logger:           logger,
	}
	return olmVariableSource, nil
}

func (r *olmVariableSource) GetVariables(ctx context.Context, source *OLMEntitySource) ([]OLMVariable, error) {
	var variables []OLMVariable
	entitySet := OLMEntitySet{}

	var start time.Time
	var elapsed time.Duration

	// collect all required package variables
	r.logger.Debug("Collecting required package variables")
	start = time.Now()
	for _, reqPkg := range r.requiredPackages {
		reqPkgVars, err := reqPkg.GetVariables(ctx, source)
		if err != nil {
			return nil, err
		}
		variables = append(variables, reqPkgVars...)
		for _, reqPkgVar := range reqPkgVars {
			for _, entity := range reqPkgVar.OrderedEntities() {
				if _, ok := entitySet[entity.ID()]; !ok {
					entitySet[entity.ID()] = entity
				}
			}
		}
	}
	elapsed = time.Since(start)
	r.logger.Debugf("took %s", elapsed)

	// collect bundles and dependencies
	r.logger.Debug("Collecting bundles and dependencies")
	start = time.Now()
	entities := make([]store.CachedBundle, 0, len(entitySet))
	for _, entity := range entitySet {
		entities = append(entities, entity)
	}
	bundleVariableSource := NewBundleVariableSource(entities...)
	bundleVariables, err := bundleVariableSource.GetVariables(ctx, source)
	if err != nil {
		return nil, err
	}
	variables = append(variables, bundleVariables...)
	for _, v := range bundleVariables {
		for _, entity := range v.OrderedEntities() {
			if _, ok := entitySet[entity.ID()]; !ok {
				entitySet[entity.ID()] = entity
			}
		}
	}
	elapsed = time.Since(start)
	r.logger.Debugf("took %s", elapsed)

	// collect uniqueness variables
	r.logger.Debug("Applying global constraints")
	start = time.Now()
	uniquenessVariableSource := NewUniquenessVariableSource()
	uniquenessVariables, err := uniquenessVariableSource.GetVariables(ctx, NewIterableEntitySource("packageSet", entitySet))
	if err != nil {
		return nil, err
	}
	variables = append(variables, uniquenessVariables...)
	elapsed = time.Since(start)
	r.logger.Debugf("took %s", elapsed)
	return variables, nil
}
