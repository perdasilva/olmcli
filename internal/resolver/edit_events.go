package resolver

import "fmt"

const (
	EditTypeVariable   = "VariableEdit"
	EditTypeConstraint = "ConstraintEdit"

	ActionAdd    = "Add"
	ActionRemove = "Remove"
	ActionUpsert = "Upsert"

	ConstraintTypeMandatory    = "Mandatory"
	ConstraintTypeProhibited   = "Prohibited"
	ConstraintTypeConflict     = "Conflict"
	ConstraintTypeDependency   = "Dependency"
	ConstraintTypeUnDependency = "UnDependency"
	ConstraintTypeAtMost       = "AtMost"
)

type Edit struct {
	Type           string                 `json:"type"`
	Action         string                 `json:"action"`
	VariableID     string                 `json:"variableId"`
	Kind           string                 `json:"kind"`
	ConstraintID   string                 `json:"constraintId,omitempty"`
	ConstraintType string                 `json:"constraintType,omitempty"`
	Params         map[string]interface{} `json:"params,omitempty"`
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
				if e.Params["dependentVariableId"] != nil {
					//if e.Params["dependentVariableId"] == "olm-bundle/catalog/instana-agent-operator/stable/instana-agent-operator.v2.0.1" {
					//	fmt.Println("adding dependency")
					//}
					dependentVariable, err := resolution.GetVariable(e.Params["dependentVariableId"].(string))
					if err != nil {
						return err
					}
					if err := variable.AddDependency(e.ConstraintID, *dependentVariable); err != nil {
						return err
					}
				}
				if e.Params["orderPreference"] != nil {
					orderPreference := e.Params["orderPreference"].(string)
					if err != nil {
						return err
					}
					if err := variable.AddDependencySort(e.ConstraintID, orderPreference); err != nil {
						return err
					}
				}
			} else if e.ConstraintType == ConstraintTypeUnDependency {
				if e.Params["unDependentVariableId"] != nil {
					dependentVariable, err := resolution.GetVariable(e.Params["unDependentVariableId"].(string))
					if err != nil {
						return err
					}
					if err := variable.AddUnDependency(e.ConstraintID, *dependentVariable); err != nil {
						return err
					}
				}
				if e.Params["orderPreference"] != nil {
					orderPreference := e.Params["orderPreference"].(string)
					if err != nil {
						return err
					}
					if err := variable.AddUnDependencySort(e.ConstraintID, orderPreference); err != nil {
						return err
					}
				}
			} else if e.ConstraintType == ConstraintTypeAtMost {
				if e.Params["n"] != nil {
					var n int
					switch t := e.Params["n"].(type) {
					case float64:
						n = int(t)
					case int:
						n = t
					case int64:
						n = int(t)
					case int32:
						n = int(t)
					case int16:
						n = int(t)
					case int8:
						n = int(t)
					case float32:
						n = int(t)
					default:
						return fmt.Errorf("unknown type for n: %T", t)
					}
					if err := variable.AddAtMostN(e.ConstraintID, n); err != nil {
						return err
					}
				}

				if e.Params["variableId"] != nil {
					dependentVariable, err := resolution.GetVariable(e.Params["variableId"].(string))
					if err != nil {
						return err
					}
					if err := variable.AddAtMostVariable(e.ConstraintID, *dependentVariable); err != nil {
						return err
					}
				}

				if e.Params["orderPreference"] != nil {
					orderPreference := e.Params["orderPreference"].(string)
					if err != nil {
						return err
					}
					if err := variable.AddAtMostSort(e.ConstraintID, orderPreference); err != nil {
						return err
					}
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
				variable.RemoveConflict(e.Params["conflictingVariableId"].(string))
			} else if e.ConstraintType == ConstraintTypeDependency {
				if e.Params["dependentVariableId"] == "" || e.Params["dependentVariableId"] == nil {
					variable.RemoveConstraint(e.ConstraintID)
					return nil
				}
				dependentVariable, err := resolution.GetVariable(e.Params["dependentVariableId"].(string))
				if err != nil {
					return err
				}
				if err := variable.RemoveDependency(e.ConstraintID, *dependentVariable); err != nil {
					return err
				}
			} else if e.ConstraintType == ConstraintTypeUnDependency {
				if e.Params["unDependentVariableId"] == "" || e.Params["unDependentVariableId"] == nil {
					variable.RemoveConstraint(e.ConstraintID)
					return nil
				}
				unDependentVariable, err := resolution.GetVariable(e.Params["unDependentVariableId"].(string))
				if err != nil {
					return err
				}
				if err := variable.RemoveUnDependency(e.ConstraintID, *unDependentVariable); err != nil {
					return err
				}
			} else if e.ConstraintType == ConstraintTypeAtMost {
				if e.Params["variableId"] == "" || e.Params["variableId"] == nil {
					variable.RemoveConstraint(e.ConstraintID)
					return nil
				}
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
		ConstraintTypeAtMost:       {},
		ConstraintTypeConflict:     {},
		ConstraintTypeDependency:   {},
		ConstraintTypeMandatory:    {},
		ConstraintTypeProhibited:   {},
		ConstraintTypeUnDependency: {},
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
		if e.Params == nil {
			return fmt.Errorf("missing parameters")
		}
	case ConstraintTypeConflict:
		if _, ok := e.Params["conflictingVariableId"]; !ok {
			return fmt.Errorf("missing parameter conflictingVariableId")
		}
	case ConstraintTypeDependency:
		if e.Params == nil {
			return fmt.Errorf("missing parameters")
		}
	case ConstraintTypeUnDependency:
		if e.Params == nil {
			return fmt.Errorf("missing parameters")
		}
	}

	return nil
}
