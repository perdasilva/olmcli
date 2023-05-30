package utils

import (
	"github.com/operator-framework/deppy/pkg/deppy"
	"github.com/operator-framework/deppy/pkg/deppy/constraint"
	"github.com/perdasilva/olmcli/internal/store"
)

type OLMVariable interface {
	deppy.Variable
	OrderedEntities() []store.CachedBundle
}

var _ deppy.Variable = &olmVariable{}

type olmVariable struct {
	ID              deppy.Identifier `json:"variableID"`
	orderedEntities []store.CachedBundle
	constraints     []deppy.Constraint
}

func (v olmVariable) Identifier() deppy.Identifier {
	return v.ID
}

func (v olmVariable) Constraints() []deppy.Constraint {
	return v.constraints
}

func (v olmVariable) OrderedEntities() []store.CachedBundle {
	return v.orderedEntities
}

func NewRequiredPackageVariable(id deppy.Identifier, orderedEntities ...store.CachedBundle) OLMVariable {
	constraints := []deppy.Constraint{
		constraint.Mandatory(),
	}
	if len(orderedEntities) > 0 {
		constraints = append(constraints, constraint.Dependency(toIdentifierIDs(orderedEntities)...))
	}
	return &olmVariable{
		ID:              id,
		orderedEntities: orderedEntities,
		constraints:     constraints,
	}
}

func NewUniquenessVariable(id deppy.Identifier, orderedEntities ...store.CachedBundle) OLMVariable {
	var constraints []deppy.Constraint
	if len(orderedEntities) > 0 {
		constraints = []deppy.Constraint{
			constraint.AtMost(1, toIdentifierIDs(orderedEntities)...),
		}
	}
	return &olmVariable{
		ID:              id,
		orderedEntities: orderedEntities,
		constraints:     constraints,
	}
}

var _ deppy.Variable = &BundleVariable{}

type BundleVariable struct {
	*store.CachedBundle
	orderedDependencies []store.CachedBundle
	constraints         []deppy.Constraint
}

func NewBundleVariable(entity *store.CachedBundle, orderedDependencies ...store.CachedBundle) OLMVariable {
	var constraints []deppy.Constraint
	if len(orderedDependencies) > 0 {
		constraints = []deppy.Constraint{
			constraint.Dependency(toIdentifierIDs(orderedDependencies)...),
		}
	}
	return &BundleVariable{
		CachedBundle:        entity,
		orderedDependencies: orderedDependencies,
		constraints:         constraints,
	}
}

func (b BundleVariable) Identifier() deppy.Identifier {
	return deppy.Identifier(b.BundleID)
}

func (b BundleVariable) Constraints() []deppy.Constraint {
	return b.constraints
}

func (b BundleVariable) OrderedEntities() []store.CachedBundle {
	return b.orderedDependencies
}

func toIdentifierIDs(entities []store.CachedBundle) []deppy.Identifier {
	ids := make([]deppy.Identifier, len(entities))
	for index, _ := range entities {
		ids[index] = deppy.Identifier(entities[index].BundleID)
	}
	return ids
}
