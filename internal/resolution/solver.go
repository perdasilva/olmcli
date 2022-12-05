package resolution

import (
	"context"

	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
)

type OLMSolver struct {
	olmEntitySource *OLMEntitySource
}

func NewOLMSolver(packageDB store.PackageDatabase) *OLMSolver {
	return &OLMSolver{
		olmEntitySource: &OLMEntitySource{
			packageDB,
		},
	}
}

func (s *OLMSolver) Solve(ctx context.Context, requiredPackages ...RequiredPackage) (v2.Solution, error) {
	variableSource, err := OLMVariableSource(requiredPackages)
	if err != nil {
		return nil, err
	}
	deppySolver, err := v2.NewDeppySolver[OLMEntity, OLMVariable, *OLMEntitySource](s.olmEntitySource, variableSource)
	if err != nil {
		return nil, err
	}
	return deppySolver.Solve(ctx)
}
