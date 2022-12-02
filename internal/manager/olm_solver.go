package manager

import (
	"context"

	deppyconstraint "github.com/operator-framework/deppy/pkg/constraints"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/deppy/pkg/solver"
	"github.com/perdasilva/olmcli/internal/resolution/constraints"
	"github.com/perdasilva/olmcli/internal/store"
)

type OLMResolver struct {
	resolver               solver.Solver
	olmConstraintGenerator *constraints.OLMConstraintGenerator
}

func (o *OLMResolver) SolveFor(ctx context.Context, requiredPackageConstraints ...constraints.RequirePackage) (solver.Solution, error) {
	o.olmConstraintGenerator.SetRequiredPackageConstraints(requiredPackageConstraints...)
	return o.resolver.Solve(ctx)
}

func NewOLMResolver(packageDatabase store.PackageDatabase) (*OLMResolver, error) {
	olmConstraintGenerator := constraints.NewOLMConstraintGenerator()
	resolver, err := solver.NewDeppySolver(
		entitysource.NewGroup(NewPackageDatabaseEntitySource(packageDatabase)),
		deppyconstraint.NewConstraintAggregator(olmConstraintGenerator, &constraints.EntityConstraints{}, &constraints.GVKUniqueness{}, &constraints.PkgUniqueness{}),
	)
	if err != nil {
		return nil, err
	}
	return &OLMResolver{
		olmConstraintGenerator: olmConstraintGenerator,
		resolver:               resolver,
	}, nil
}
