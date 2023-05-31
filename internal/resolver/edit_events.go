package resolver

import "fmt"

const (
	EditTypeVariable   = "VariableEdit"
	EditTypeConstraint = "ConstraintEdit"

	ActionAdd    = "Add"
	ActionRemove = "Remove"
	ActionUpsert = "Upsert"

	ConstraintTypeMandatory  = "Mandatory"
	ConstraintTypeProhibited = "Prohibited"
	ConstraintTypeConflict   = "Conflict"
	ConstraintTypeDependency = "Dependency"
	ConstraintTypeAtMost     = "AtMost"
)

type Edit struct {
	Type           string                 `yaml:"type"`
	Action         string                 `yaml:"action"`
	VariableID     string                 `yaml:"variableId"`
	Kind           string                 `yaml:"kind"`
	ConstraintID   string                 `yaml:"constraintId,omitempty"`
	ConstraintType string                 `yaml:"constraintType,omitempty"`
	Params         map[string]interface{} `yaml:"params,omitempty"`
}

func (e *Edit) Execute(resolution Resolution) error {
	if err := e.validate(); err != nil {
		return err
	}

	id := e.VariableID
	kind := e.Kind

	switch e.Type {
	case EditTypeVariable:
		var properties map[string]interface{}
		if _, ok := e.Params["properties"]; ok {
			properties = e.Params["properties"].(map[string]interface{})
		}

		if e.Action == ActionAdd {
			if _, err := resolution.NewVariable(id, kind, properties); err != nil {
				return err
			}
		} else if e.Action == ActionRemove {
			// not yet supported
		} else if e.Action == ActionUpsert {
			if _, err := resolution.UpsertVariable(id, kind, properties); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unknown action: %s", e.Action)
		}
	case EditTypeConstraint:
		variable, err := resolution.GetVariable(id)
		if err != nil {
			return err
		}
		if e.Action == ActionAdd {
			if e.ConstraintType == ConstraintTypeMandatory {
				variable.AddMandatory()
			} else if e.ConstraintType == ConstraintTypeProhibited {
				variable.AddProhibited()
			} else if e.ConstraintType == ConstraintTypeConflict {
				conflictingVariable, err := resolution.GetVariable(e.Params["conflictingVariableId"].(string))
				if err != nil {
					return err
				}
				variable.AddConflict(*conflictingVariable)
			} else if e.ConstraintType == ConstraintTypeDependency {
				dependentVariable, err := resolution.GetVariable(e.Params["dependentVariableId"].(string))
				if err != nil {
					return err
				}
				if err := variable.AddDependency(e.ConstraintID, *dependentVariable); err != nil {
					return err
				}
			} else if e.ConstraintType == ConstraintTypeAtMost {
				n := e.Params["n"].(int)
				dependentVariable, err := resolution.GetVariable(e.Params["variableId"].(string))
				if err != nil {
					return err
				}
				if err := variable.AddAtMost(e.ConstraintID, n, *dependentVariable); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("unknown constraint type: %s", e.ConstraintType)
			}
		} else if e.Action == ActionRemove {
			if e.ConstraintType == ConstraintTypeMandatory {
				variable.RemoveMandatory()
			} else if e.ConstraintType == ConstraintTypeProhibited {
				variable.RemoveProhibited()
			} else if e.ConstraintType == ConstraintTypeConflict {
				variable.RemoveConflict(e.Params["conflicting_variable_id"].(string))
			} else if e.ConstraintType == ConstraintTypeDependency {
				dependentVariable, err := resolution.GetVariable(e.Params["dependent_variable_id"].(string))
				if err != nil {
					return err
				}
				if err := variable.RemoveDependency(e.ConstraintID, *dependentVariable); err != nil {
					return err
				}
			} else if e.ConstraintType == ConstraintTypeAtMost {
				variable, err := resolution.GetVariable(e.Params["variableId"].(string))
				if err != nil {
					return err
				}
				if err := variable.RemoveAtMost(e.ConstraintID, *variable); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("unknown constraint type: %s", e.ConstraintType)
			}
		}
	default:
		return fmt.Errorf("unknown edit type: %s", e.Type)
	}
	return nil
}

func (e *Edit) validate() error {
	validTypes := map[string]struct{}{EditTypeVariable: {}, EditTypeConstraint: {}}
	validActions := map[string]struct{}{ActionAdd: {}, ActionRemove: {}, ActionUpsert: {}}
	validConstraintTypes := map[string]struct{}{
		ConstraintTypeAtMost:     {},
		ConstraintTypeConflict:   {},
		ConstraintTypeDependency: {},
		ConstraintTypeMandatory:  {},
		ConstraintTypeProhibited: {},
	}

	if e.VariableID == "" {
		return fmt.Errorf("missing variableId")
	}

	if e.Kind == "" {
		return fmt.Errorf("missing kind")
	}

	if _, ok := validTypes[e.Type]; !ok {
		return fmt.Errorf("invalid edit type: %s", e.Type)
	}

	if _, ok := validActions[e.Action]; !ok {
		return fmt.Errorf("invalid action: %s", e.Action)
	}

	if e.Type == EditTypeConstraint {
		requiresConstraintId := e.ConstraintType == ConstraintTypeConflict || e.ConstraintType == ConstraintTypeDependency || e.ConstraintType == ConstraintTypeAtMost
		if e.ConstraintID == "" && requiresConstraintId {
			return fmt.Errorf("missing constraintId")
		}
		if _, ok := validConstraintTypes[e.ConstraintType]; !ok {
			return fmt.Errorf("invalid constraint type: %s", e.ConstraintType)
		}
	} else {
		if e.ConstraintType != "" {
			return fmt.Errorf("constraint type is only valid for %s", EditTypeConstraint)
		}
	}

	switch e.ConstraintType {
	case ConstraintTypeAtMost:
		if _, ok := e.Params["n"]; !ok && e.Action == ActionAdd {
			return fmt.Errorf("missing parameter n")
		}
		if _, ok := e.Params["variableId"]; !ok {
			return fmt.Errorf("missing parameter variableId")
		}
	case ConstraintTypeConflict:
		if _, ok := e.Params["conflictingVariableId"]; !ok {
			return fmt.Errorf("missing parameter conflictingVariableId")
		}
	case ConstraintTypeDependency:
		if _, ok := e.Params["dependentVariableId"]; !ok {
			return fmt.Errorf("missing parameter dependentVariableId")
		}
	}

	return nil
}
