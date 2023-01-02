package resolution

import (
	"context"

	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/pipeline/required_package"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
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
	olmEntitySource *utils.OLMEntitySource
	logger          *logrus.Logger
}

func NewOLMSolver(packageDB store.PackageDatabase, logger *logrus.Logger) *OLMSolver {
	return &OLMSolver{
		olmEntitySource: &utils.OLMEntitySource{
			packageDB,
		},
		logger: logger,
	}
}

func (s *OLMSolver) Solve(ctx context.Context, requiredPackages ...*RequiredPackage) ([]Installable, error) {
	//variableSource, err := OLMVariableSource(requiredPackages, s.logger)
	//if err != nil {
	//	return nil, err
	//}
	//deppySolver, err := v2.NewDeppySolver[*store.CachedBundle, OLMVariable, *OLMEntitySource](s.olmEntitySource, variableSource)
	//if err != nil {
	//	return nil, err
	//}
	//solution, err := deppySolver.Solve(ctx)
	//if err != nil {
	//	return nil, err
	//}
	resolutionPipeline := pipeline.NewResolutionPipeline(s.olmEntitySource)
	var rpps []*required_package.RequiredPackageProducer
	for _, rp := range requiredPackages {
		rpp, err := required_package.NewRequiredPackageProducer(rp.packageName, s.olmEntitySource)
		if err != nil {
			return nil, err
		}
		rpps = append(rpps, rpp)
	}
	solution, err := resolutionPipeline.Execute(ctx, rpps...)
	if err != nil {
		return nil, err
	}

	selectedVariables := map[string]*utils.BundleVariable{}
	for _, variable := range solution {
		switch v := variable.(type) {
		case *utils.BundleVariable:
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
	utils.Sort(installables, byTopology)
	return installables, nil
}
