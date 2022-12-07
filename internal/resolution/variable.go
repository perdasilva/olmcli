package resolution

import (
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/store"
)

type OLMVariable interface {
	sat.Variable
	OrderedEntities() []store.CachedBundle
}

var _ sat.Variable = &olmVariable{}

type olmVariable struct {
	id              sat.Identifier
	orderedEntities []store.CachedBundle
	constraints     []sat.Constraint
}

func (v olmVariable) Identifier() sat.Identifier {
	return v.id
}

func (v olmVariable) Constraints() []sat.Constraint {
	return v.constraints
}

func (v olmVariable) OrderedEntities() []store.CachedBundle {
	return v.orderedEntities
}

func NewRequiredPackageVariable(id sat.Identifier, orderedEntities ...store.CachedBundle) OLMVariable {
	constraints := []sat.Constraint{
		sat.Mandatory(),
	}
	if len(orderedEntities) > 0 {
		constraints = append(constraints, sat.Dependency(toIdentifierIDs(orderedEntities)...))
	}
	return &olmVariable{
		id:              id,
		orderedEntities: orderedEntities,
		constraints:     constraints,
	}
}

func NewUniquenessVariable(id sat.Identifier, orderedEntities ...store.CachedBundle) OLMVariable {
	var constraints []sat.Constraint
	if len(orderedEntities) > 0 {
		constraints = []sat.Constraint{
			sat.AtMost(1, toIdentifierIDs(orderedEntities)...),
		}
	}
	return &olmVariable{
		id:              id,
		orderedEntities: orderedEntities,
		constraints:     constraints,
	}
}

var _ sat.Variable = &BundleVariable{}

type BundleVariable struct {
	*store.CachedBundle
	orderedDependencies []store.CachedBundle
	constraints         []sat.Constraint
}

func NewBundleVariable(entity *store.CachedBundle, orderedDependencies ...store.CachedBundle) OLMVariable {
	var constraints []sat.Constraint
	if len(orderedDependencies) > 0 {
		constraints = []sat.Constraint{
			sat.Dependency(toIdentifierIDs(orderedDependencies)...),
		}
	}
	return &BundleVariable{
		CachedBundle:        entity,
		orderedDependencies: orderedDependencies,
		constraints:         constraints,
	}
}

func (b BundleVariable) Identifier() sat.Identifier {
	return sat.Identifier(b.BundleID)
}

func (b BundleVariable) Constraints() []sat.Constraint {
	return b.constraints
}

func (b BundleVariable) OrderedEntities() []store.CachedBundle {
	return b.orderedDependencies
}

func toIdentifierIDs(entities []store.CachedBundle) []sat.Identifier {
	ids := make([]sat.Identifier, len(entities))
	for index, _ := range entities {
		ids[index] = sat.Identifier(entities[index].BundleID)
	}
	return ids
}
