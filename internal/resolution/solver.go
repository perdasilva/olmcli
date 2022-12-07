package resolution

import (
	"context"

	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/sirupsen/logrus"
)

type Installable struct {
	store.CachedBundle
	Dependencies map[string]store.CachedBundle
}

func byTopology(i1 *Installable, i2 *Installable) bool {
	if _, ok := i2.Dependencies[i1.BundleID]; ok {
		return true
	}
	if _, ok := i1.Dependencies[i2.BundleID]; ok {
		return false
	}

	if len(i1.Dependencies) == len(i2.Dependencies) {
		return i1.BundleID < i2.BundleID
	}

	return len(i1.Dependencies) < len(i2.Dependencies)
}

type OLMSolver struct {
	olmEntitySource *OLMEntitySource
	logger          *logrus.Logger
}

func NewOLMSolver(packageDB store.PackageDatabase, logger *logrus.Logger) *OLMSolver {
	return &OLMSolver{
		olmEntitySource: &OLMEntitySource{
			packageDB,
		},
		logger: logger,
	}
}

func (s *OLMSolver) Solve(ctx context.Context, requiredPackages ...*RequiredPackage) ([]Installable, error) {
	variableSource, err := OLMVariableSource(requiredPackages, s.logger)
	if err != nil {
		return nil, err
	}
	deppySolver, err := v2.NewDeppySolver[*store.CachedBundle, OLMVariable, *OLMEntitySource](s.olmEntitySource, variableSource)
	if err != nil {
		return nil, err
	}
	solution, err := deppySolver.Solve(ctx)
	if err != nil {
		return nil, err
	}

	selectedVariables := map[string]*BundleVariable{}
	for _, variable := range solution {
		switch v := variable.(type) {
		case *BundleVariable:
			selectedVariables[v.BundleID] = v
		}
	}
	var installables []Installable
	for _, variable := range selectedVariables {
		dependencies := map[string]store.CachedBundle{}
		for _, dependency := range variable.OrderedEntities() {
			if _, ok := selectedVariables[dependency.BundleID]; ok {
				dependencies[dependency.BundleID] = dependency
			}
		}
		installables = append(installables, Installable{
			CachedBundle: *variable.CachedBundle,
			Dependencies: dependencies,
		})
	}
	Sort(installables, byTopology)
	return installables, nil
}
