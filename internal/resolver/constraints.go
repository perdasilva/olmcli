package resolver

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/antonmedv/expr"
	"github.com/blang/semver/v4"
	"github.com/go-air/gini/z"
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

type UnDependencyConstraint struct {
	undependencyIDs []deppy.Identifier
}

func (constraint *UnDependencyConstraint) String(subject deppy.Identifier) string {
	if len(constraint.undependencyIDs) == 0 {
		return fmt.Sprintf("%s has an undependency without any candidates to satisfy it", subject)
	}
	s := make([]string, len(constraint.undependencyIDs))
	for i, each := range constraint.undependencyIDs {
		s[i] = string(each)
	}
	return fmt.Sprintf("%s requires at least one of %s must be false", subject, strings.Join(s, ", "))
}

func (constraint *UnDependencyConstraint) Apply(lm deppy.LitMapping, subject deppy.Identifier) z.Lit {
	m := lm.LitOf(subject).Not()
	for _, each := range constraint.undependencyIDs {
		m = lm.LogicCircuit().Or(m, lm.LitOf(each).Not())
	}
	return m
}

func (constraint *UnDependencyConstraint) DependencyIDs() []deppy.Identifier {
	return constraint.undependencyIDs
}

func (constraint *UnDependencyConstraint) Order() []deppy.Identifier {
	return constraint.undependencyIDs
}

func (constraint *UnDependencyConstraint) Anchor() bool {
	return false
}

func ConflictConstraintID(conflictingVariableID string) string {
	return fmt.Sprintf("confict/%s", conflictingVariableID)
}

type Constraint interface {
	deppy.Constraint
	ConstraintID() string
	Kind() string
	MarshalJSON() ([]byte, error)
	Sort() error
}

var _ Constraint = &MandatoryConstraint{}

type MandatoryConstraint struct {
	deppy.Constraint
}

func (m MandatoryConstraint) Sort() error {
	return nil
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

func (p ProhibitedConstraint) Sort() error {
	return nil
}

func (p ProhibitedConstraint) Kind() string {
	return ProhibitedConstraintKind
}

func (p ProhibitedConstraint) ConstraintID() string {
	return ProhibitedConstraintID
}

func (p ProhibitedConstraint) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		ID   string `json:"id"`
		Kind string `json:"kind"`
	}{
		ID:   p.ConstraintID(),
		Kind: p.Kind(),
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

func (c ConflictConstraint) Sort() error {
	return nil
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
	dependencies    map[deppy.Identifier]Variable
	orderPreference string
}

func (d *DependencyConstraint) Kind() string {
	return DependencyConstraintKind
}

func (d *DependencyConstraint) ConstraintID() string {
	return d.constraintID
}

func (d *DependencyConstraint) MarshalJSON() ([]byte, error) {
	depIds, err := variablesInPreferenceOrder(d.dependencies, d.orderPreference)
	if err != nil {
		return nil, err
	}
	return json.Marshal(&struct {
		ID            string             `json:"id"`
		Kind          string             `json:"kind"`
		DependencyIDs []deppy.Identifier `json:"undependencyIDs"`
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
	return &DependencyConstraint{constraintID, constraint.Dependency(toIdentifierIDs(deps)...), deps, ""}
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

func (d *DependencyConstraint) Sort() error {
	if d.orderPreference == "" {
		return nil
	}
	depIds, err := variablesInPreferenceOrder(d.dependencies, d.orderPreference)
	if err != nil {
		return err
	}

	d.Constraint = constraint.Dependency(depIds...)
	return nil
}

var _ Constraint = &UnDependencyConstr{}

type UnDependencyConstr struct {
	constraintID string
	deppy.Constraint
	dependencies    map[deppy.Identifier]Variable
	orderPreference string
}

func (d *UnDependencyConstr) Kind() string {
	return DependencyConstraintKind
}

func (d *UnDependencyConstr) ConstraintID() string {
	return d.constraintID
}

func (d *UnDependencyConstr) MarshalJSON() ([]byte, error) {
	depIds, err := variablesInPreferenceOrder(d.dependencies, d.orderPreference)
	if err != nil {
		return nil, err
	}
	return json.Marshal(&struct {
		ID              string             `json:"id"`
		Kind            string             `json:"kind"`
		UnDependencyIDs []deppy.Identifier `json:"undependencyIDs"`
	}{
		ID:              d.ConstraintID(),
		Kind:            d.Kind(),
		UnDependencyIDs: depIds,
	})
}

func UnDependency(constraintID string, dependencies ...Variable) Constraint {
	deps := make(map[deppy.Identifier]Variable)
	for _, d := range dependencies {
		deps[d.Identifier()] = d
	}
	return &UnDependencyConstr{constraintID, &UnDependencyConstraint{toIdentifierIDs(deps)}, deps, ""}
}

func (d *UnDependencyConstr) AddUnDependency(dependentVariable Variable) {
	if _, ok := d.dependencies[dependentVariable.Identifier()]; !ok {
		d.dependencies[dependentVariable.Identifier()] = dependentVariable
		d.Constraint = &UnDependencyConstraint{toIdentifierIDs(d.dependencies)}
	}
}

func (d *UnDependencyConstr) RemoveUnDependency(dependentVariable Variable) {
	if _, ok := d.dependencies[dependentVariable.Identifier()]; ok {
		delete(d.dependencies, dependentVariable.Identifier())
		d.Constraint = &UnDependencyConstraint{toIdentifierIDs(d.dependencies)}
	}
}

func (d *UnDependencyConstr) Sort() error {
	if d.orderPreference == "" {
		return nil
	}
	depIds, err := variablesInPreferenceOrder(d.dependencies, d.orderPreference)
	if err != nil {
		return err
	}

	d.Constraint = &UnDependencyConstraint{depIds}
	return nil
}

var _ Constraint = &AtMostConstraint{}

type AtMostConstraint struct {
	constraintID string
	deppy.Constraint
	n               int
	variables       map[deppy.Identifier]Variable
	orderPreference string
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

func (a *AtMostConstraint) Sort() error {
	if a.orderPreference == "" {
		return nil
	}
	varIds, err := variablesInPreferenceOrder(a.variables, a.orderPreference)
	if err != nil {
		return err
	}

	a.Constraint = constraint.AtMost(a.n, varIds...)
	return nil
}

func (a *AtMostConstraint) MarshalJSON() ([]byte, error) {
	varIds, err := variablesInPreferenceOrder(a.variables, a.orderPreference)
	if err != nil {
		return nil, err
	}
	return json.Marshal(&struct {
		ID        string             `json:"id"`
		Kind      string             `json:"kind"`
		N         int                `json:"n"`
		Variables []deppy.Identifier `json:"variableIDs"`
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
	return &AtMostConstraint{constraintID, constraint.AtMost(n, toIdentifierIDs(vars)...), n, vars, ""}
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

func variablesInPreferenceOrder(variables map[deppy.Identifier]Variable, orderPreference string) ([]deppy.Identifier, error) {
	sortFn := strings.Trim(orderPreference, "{{}} ")
	vars := make([]Variable, 0, len(variables))
	for _, v := range variables {
		vars = append(vars, v)
	}

	if sortFn != "" {
		program, err := expr.Compile(sortFn, expr.AsBool())
		if err != nil {
			return nil, FatalError(fmt.Sprintf("failed to compile preference order function: %s", err))
		}
		var outerError error
		sort.Slice(vars, func(i, j int) bool {
			result, err := expr.Run(program, map[string]interface{}{
				"v1": vars[i],
				"v2": vars[j],
				"semverCompare": func(a string, b string) (int, error) {
					left, err := semver.Parse(a)
					if err != nil {
						return 0, err
					}
					right, err := semver.Parse(b)
					if err != nil {
						return 0, err
					}
					if left.GT(right) {
						return 1, nil
					}
					if left.LT(right) {
						return -1, nil
					}
					return 0, nil
				},
				"weightedCompare": func(prefOrder []interface{}, a string, b string) (int, error) {
					wA := 0
					wB := 0
					for i, pref := range prefOrder {
						if pref == a {
							wA = i + 1
						}
						if pref == b {
							wB = i + 1
						}
					}
					return wA - wB, nil
				},
				"multiSort": func(cmp ...int) (int, error) {
					for _, fn := range cmp {
						if fn != 0 {
							return fn, nil
						}
					}
					return 0, nil
				},
			})
			if err != nil {
				outerError = FatalError(fmt.Sprintf("failed to run preference order function: %s", err))
				return false
			}
			return result.(bool)
		})

		if outerError != nil {
			return nil, outerError
		}
	}

	varIds := make([]deppy.Identifier, 0, len(vars))
	for _, v := range vars {
		varIds = append(varIds, v.Identifier())
	}

	return varIds, nil
}
