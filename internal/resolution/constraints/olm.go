package constraints

import (
	"context"
	"fmt"

	. "github.com/operator-framework/deppy/pkg/constraints"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"github.com/operator-framework/deppy/pkg/sat"
)

var _ ConstraintGenerator = &OLMConstraintGenerator{}

type OLMConstraintGenerator struct {
	requiredPackageConstraints []RequirePackage
}

func NewOLMConstraintGenerator() *OLMConstraintGenerator {
	return &OLMConstraintGenerator{}
}

func (o *OLMConstraintGenerator) SetRequiredPackageConstraints(requiredPackageConstraints ...RequirePackage) {
	o.requiredPackageConstraints = requiredPackageConstraints
}

func (o *OLMConstraintGenerator) GetVariables(ctx context.Context, querier entitysource.EntityQuerier) ([]sat.Variable, error) {
	if len(o.requiredPackageConstraints) == 0 {
		return nil, nil
	}

	variables := make([]sat.Variable, 0)
	variableSet := map[sat.Identifier]struct{}{}

	// collect required packages and check for duplicates
	for _, reqPkgConstraint := range o.requiredPackageConstraints {
		vars, err := reqPkgConstraint.GetVariables(ctx, querier)
		if err != nil {
			return nil, err
		}
		for _, variable := range vars {
			if _, ok := variableSet[variable.Identifier()]; ok {
				return nil, fmt.Errorf("duplicate variable %s", variable.Identifier())
			}
			variableSet[variable.Identifier()] = struct{}{}
			variables = append(variables, variable)
		}
	}

	// return variables
	return variables, nil
}
