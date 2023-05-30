package resolver

import (
	"encoding/json"
	"fmt"

	"github.com/operator-framework/deppy/pkg/deppy"
	"github.com/operator-framework/deppy/pkg/deppy/constraint"
)

const (
	MandatoryConstraintID  = "MandatoryConstraint"
	ProhibitedConstraintID = "ProhibitedConstraint"

	MandatoryConstraintKind  = "olm.constraint.mandatory"
	ProhibitedConstraintKind = "olm.constraint.prohibited"
	DependencyConstraintKind = "olm.constraint.dependency"
	AtMostConstraintKind     = "olm.constraint.atmost"
	ConflictConstraintKind   = "olm.constraint.conflict"
)

func ConflictConstraintID(conflictingVariableID string) string {
	return fmt.Sprintf("confict/%s", conflictingVariableID)
}

type Constraint interface {
	deppy.Constraint
	ConstraintID() string
	Kind() string
	MarshalJSON() ([]byte, error)
}

var _ Constraint = &MandatoryConstraint{}

type MandatoryConstraint struct {
	deppy.Constraint
}

func (m MandatoryConstraint) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID   string `json:"id"`
		Kind string `json:"kind"`
	}{
		ID:   m.ConstraintID(),
		Kind: m.Kind(),
	})
}

func (m MandatoryConstraint) Kind() string {
	return MandatoryConstraintKind
}

func (m MandatoryConstraint) ConstraintID() string {
	return MandatoryConstraintID
}

func Mandatory() Constraint {
	return MandatoryConstraint{constraint.Mandatory()}
}

var _ Constraint = &ProhibitedConstraint{}

type ProhibitedConstraint struct {
	deppy.Constraint
}

func (p ProhibitedConstraint) Kind() string {
	return ProhibitedConstraintKind
}

func (p ProhibitedConstraint) ConstraintID() string {
	return ProhibitedConstraintID
}

func (m ProhibitedConstraint) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID   string `json:"id"`
		Kind string `json:"kind"`
	}{
		ID:   m.ConstraintID(),
		Kind: m.Kind(),
	})
}

func Prohibited() Constraint {
	return ProhibitedConstraint{constraint.Prohibited()}
}

var _ Constraint = &ConflictConstraint{}

type ConflictConstraint struct {
	deppy.Constraint
	conflictingVariable Variable
}

func (c ConflictConstraint) Kind() string {
	return ConflictConstraintKind
}

func (c ConflictConstraint) ConstraintID() string {
	return ConflictConstraintID(c.conflictingVariable.Identifier().String())
}

func (c ConflictConstraint) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID                    string `json:"id"`
		Kind                  string `json:"kind"`
		ConflictingVariableID string `json:"conflictingVariableID"`
	}{
		ID:                    c.ConstraintID(),
		Kind:                  c.Kind(),
		ConflictingVariableID: c.conflictingVariable.Identifier().String(),
	})
}

func Conflict(conflictingVariable Variable) Constraint {
	return ConflictConstraint{constraint.Conflict(conflictingVariable.Identifier()), conflictingVariable}
}

var _ Constraint = &DependencyConstraint{}

type DependencyConstraint struct {
	constraintID string
	deppy.Constraint
	dependencies map[deppy.Identifier]Variable
}

func (d *DependencyConstraint) Kind() string {
	return DependencyConstraintKind
}

func (d *DependencyConstraint) ConstraintID() string {
	return d.constraintID
}

func (d *DependencyConstraint) MarshalJSON() ([]byte, error) {
	depIds := make([]string, 0, len(d.dependencies))
	for id := range d.dependencies {
		depIds = append(depIds, id.String())
	}
	return json.Marshal(&struct {
		ID            string   `json:"id"`
		Kind          string   `json:"kind"`
		DependencyIDs []string `json:"dependencyIDs"`
	}{
		ID:            d.ConstraintID(),
		Kind:          d.Kind(),
		DependencyIDs: depIds,
	})
}

func Dependency(constraintID string, dependencies ...Variable) Constraint {
	deps := make(map[deppy.Identifier]Variable)
	for _, d := range dependencies {
		deps[d.Identifier()] = d
	}
	return &DependencyConstraint{constraintID, constraint.Dependency(toIdentifierIDs(deps)...), deps}
}

func (d *DependencyConstraint) AddDependency(dependentVariable Variable) {
	if _, ok := d.dependencies[dependentVariable.Identifier()]; !ok {
		d.dependencies[dependentVariable.Identifier()] = dependentVariable
		d.Constraint = constraint.Dependency(toIdentifierIDs(d.dependencies)...)
	}
}

func (d *DependencyConstraint) RemoveDependency(dependentVariable Variable) {
	if _, ok := d.dependencies[dependentVariable.Identifier()]; ok {
		delete(d.dependencies, dependentVariable.Identifier())
		d.Constraint = constraint.Dependency(toIdentifierIDs(d.dependencies)...)
	}
}

var _ Constraint = &AtMostConstraint{}

type AtMostConstraint struct {
	constraintID string
	deppy.Constraint
	n         int
	variables map[deppy.Identifier]Variable
}

func (a *AtMostConstraint) Kind() string {
	return AtMostConstraintKind
}

func (a *AtMostConstraint) ConstraintID() string {
	return a.constraintID
}

func (a *AtMostConstraint) N() int {
	return a.n
}

func (a *AtMostConstraint) MarshalJSON() ([]byte, error) {
	varIds := make([]string, 0, len(a.variables))
	for id := range a.variables {
		varIds = append(varIds, id.String())
	}
	return json.Marshal(&struct {
		ID        string   `json:"id"`
		Kind      string   `json:"kind"`
		N         int      `json:"n"`
		Variables []string `json:"variableIDs"`
	}{
		ID:        a.ConstraintID(),
		Kind:      a.Kind(),
		N:         a.N(),
		Variables: varIds,
	})
}

func AtMost(constraintID string, n int, variables ...Variable) Constraint {
	vars := make(map[deppy.Identifier]Variable)
	for _, v := range variables {
		vars[v.Identifier()] = v
	}
	return &AtMostConstraint{constraintID, constraint.AtMost(n, toIdentifierIDs(vars)...), n, vars}
}

func (a *AtMostConstraint) AddVariable(variable Variable) {
	if _, ok := a.variables[variable.Identifier()]; !ok {
		a.variables[variable.Identifier()] = variable
		a.Constraint = constraint.AtMost(len(a.variables), toIdentifierIDs(a.variables)...)
	}
}

func (a *AtMostConstraint) RemoveVariable(variable Variable) {
	if _, ok := a.variables[variable.Identifier()]; ok {
		delete(a.variables, variable.Identifier())
		a.Constraint = constraint.AtMost(len(a.variables), toIdentifierIDs(a.variables)...)
	}
}

func toIdentifierIDs(variables map[deppy.Identifier]Variable) []deppy.Identifier {
	var ids []deppy.Identifier
	for _, v := range variables {
		ids = append(ids, v.Identifier())
	}
	return ids
}
