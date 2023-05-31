package resolver

import (
	"fmt"

	"github.com/operator-framework/deppy/pkg/deppy"
)

var _ deppy.Variable = &Variable{}

type Variable struct {
	VariableID     string                 `json:"id"`
	Properties     map[string]interface{} `json:"properties"`
	VarConstraints map[string]Constraint  `json:"constraints"`
}

func NewVariable(variableID string, kind string, properties map[string]interface{}) *Variable {
	if properties == nil {
		properties = make(map[string]interface{})
	}
	//if _, ok := properties["kind"]; ok {
	//	panic("properties already contains kind")
	//}
	//if _, ok := properties["id"]; ok {
	//	panic("properties already contains id")
	//}
	properties["kind"] = kind
	properties["id"] = variableID
	return &Variable{
		VariableID:     variableID,
		Properties:     properties,
		VarConstraints: map[string]Constraint{},
	}
}

func (v *Variable) Kind() string {
	return v.Properties["kind"].(string)
}

func (v *Variable) Identifier() deppy.Identifier {
	return deppy.Identifier(v.VariableID)
}

func (v *Variable) Constraints() []deppy.Constraint {
	var constraints []deppy.Constraint
	for _, c := range v.VarConstraints {
		constraints = append(constraints, c)
	}
	return constraints
}

func (v *Variable) Property(key string) interface{} {
	if v, ok := v.Properties[key]; ok {
		return v
	}
	return ""
}

func (v *Variable) AddMandatory() {
	c := Mandatory()
	v.VarConstraints[c.ConstraintID()] = c
}

func (v *Variable) RemoveMandatory() {
	v.RemoveConstraint(MandatoryConstraintID)
}

func (v *Variable) AddProhibited() {
	c := Prohibited()
	v.VarConstraints[c.ConstraintID()] = c
}

func (v *Variable) RemoveProhibited() {
	v.RemoveConstraint(ProhibitedConstraintID)
}

func (v *Variable) AddConflict(conflictingVariable Variable) {
	c := Conflict(conflictingVariable)
	v.VarConstraints[c.ConstraintID()] = c
}

func (v *Variable) RemoveConflict(conflictingVariableID string) {
	v.RemoveConstraint(ConflictConstraintID(conflictingVariableID))
}

func (v *Variable) AddDependency(constraintID string, dependentVariable Variable) error {
	if _, ok := v.VarConstraints[constraintID]; !ok {
		v.VarConstraints[constraintID] = Dependency(constraintID)
	}
	c, ok := v.VarConstraints[constraintID].(*DependencyConstraint)
	if !ok {
		return fmt.Errorf("constraint with id %s is not a DependencyConstraint", constraintID)
	}
	c.AddDependency(dependentVariable)
	return nil
}

func (v *Variable) RemoveDependency(constraintID string, dependentVariable Variable) error {
	if _, ok := v.VarConstraints[constraintID]; !ok {
		return fmt.Errorf("constraint with id %s does not exist", constraintID)
	}
	c, ok := v.VarConstraints[constraintID].(*DependencyConstraint)
	if !ok {
		return fmt.Errorf("constraint with id %s is not a Dependency constraint", constraintID)
	}
	c.RemoveDependency(dependentVariable)
	return nil
}

func (v *Variable) RemoveConstraint(constraintID string) {
	delete(v.VarConstraints, constraintID)
}

func (v *Variable) AddAtMost(constraintID string, n int, variable Variable) error {
	if _, ok := v.VarConstraints[constraintID]; !ok {
		v.VarConstraints[constraintID] = AtMost(constraintID, n)
	}
	c, ok := v.VarConstraints[constraintID].(*AtMostConstraint)
	if !ok {
		return fmt.Errorf("constraint with id %s is not an AtMost constraint", constraintID)
	}
	if n != c.n {
		return fmt.Errorf("constraint with id %s has different n than %d", constraintID, n)
	}
	c.AddVariable(variable)
	return nil
}

func (v *Variable) RemoveAtMost(constraintID string, variable Variable) error {
	if _, ok := v.VarConstraints[constraintID]; !ok {
		return fmt.Errorf("constraint with id %s does not exist", constraintID)
	}
	c, ok := v.VarConstraints[constraintID].(*AtMostConstraint)
	if !ok {
		return fmt.Errorf("constraint with id %s is not an AtMost constraint", constraintID)
	}
	c.RemoveVariable(variable)
	return nil
}
