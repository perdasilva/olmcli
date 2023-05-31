package resolver

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/operator-framework/deppy/pkg/deppy"
	"github.com/operator-framework/deppy/pkg/deppy/input"
)

type VariableSource interface {
	VariableSourceID() string
	Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error
}

type Resolver struct {
	sources map[string]VariableSource
	lock    sync.RWMutex
}

func (r *Resolver) RegisterVariableSource(source VariableSource) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.sources == nil {
		r.sources = make(map[string]VariableSource)
	}
	if _, ok := r.sources[source.VariableSourceID()]; ok {
		panic("variable source already registered")
	}
	r.sources[source.VariableSourceID()] = source
}

type Resolution interface {
	input.VariableSource
	GetVariable(variableID string) (*Variable, error)
	NewVariable(variableID string, kind string, properties map[string]interface{}) (*Variable, error)
	UpsertVariable(variableID string, kind string, properties map[string]interface{}) (*Variable, error)
}

type resolution struct {
	sources       map[string]VariableSource
	variables     map[deppy.Identifier]*Variable
	variableQueue []*Variable
}

func (rs *resolution) GetVariables(ctx context.Context, entitySource input.EntitySource) ([]deppy.Variable, error) {
	if err := rs.gatherVariables(ctx); err != nil {
		return nil, err
	}
	variables := make([]deppy.Variable, 0, len(rs.variables))
	for _, v := range rs.variables {
		variables = append(variables, v)
	}
	return variables, nil
}

func NewResolution(sources ...VariableSource) Resolution {
	sourceMap := make(map[string]VariableSource)
	for _, source := range sources {
		sourceMap[source.VariableSourceID()] = source
	}
	return &resolution{
		sources:   sourceMap,
		variables: make(map[deppy.Identifier]*Variable),
	}
}

func (rs *resolution) GetVariable(variableID string) (*Variable, error) {
	if v, ok := rs.variables[deppy.Identifier(variableID)]; ok {
		return v, nil
	}
	return nil, NotFoundError(variableID)
}

func (rs *resolution) NewVariable(variableID string, kind string, properties map[string]interface{}) (*Variable, error) {
	if v, ok := rs.variables[deppy.Identifier(variableID)]; ok {
		return nil, ConflictError(fmt.Sprintf("variable %s already exists with kind %s", variableID, v.Kind()))
	}
	return rs.updateVariable(NewVariable(variableID, kind, properties)), nil
}

func (rs *resolution) updateVariable(variable *Variable) *Variable {
	if variable != nil {
		rs.variables[variable.Identifier()] = variable
	}
	rs.variableQueue = append(rs.variableQueue, variable)
	return variable
}

func (rs *resolution) HasVariable(variableID string) bool {
	_, ok := rs.variables[deppy.Identifier(variableID)]
	return ok
}

func (rs *resolution) UpsertVariable(variableID string, kind string, properties map[string]interface{}) (*Variable, error) {
	curVar, err := rs.GetVariable(variableID)
	if err != nil {
		if err != NotFoundError(variableID) {
			return nil, err
		}
		return rs.NewVariable(variableID, kind, properties)
	}

	if curVar.Kind() != kind {
		return nil, ConflictError(fmt.Sprintf("variable %s already exists with kind %s", variableID, curVar.Kind()))
	}

	// no properties to merge
	if properties == nil {
		return curVar, nil
	}

	// merge properties
	mergedProperties := make(map[string]interface{})
	for k, v := range curVar.Properties {
		mergedProperties[k] = v
	}

	propertiesChanged := false
	for k, v := range properties {
		if _, ok := mergedProperties[k]; !ok {
			mergedProperties[k] = v
			propertiesChanged = true
		} else {
			if !reflect.DeepEqual(v, mergedProperties[k]) {
				return nil, ConflictError(fmt.Sprintf("conflict: setting property '%s' (value=%s) for variable '%s' to %s", k, v, variableID, mergedProperties[k]))
			}
		}
	}

	if propertiesChanged {
		variable := NewVariable(variableID, kind, mergedProperties)
		return rs.updateVariable(variable), nil
	}
	return curVar, nil
}

func (rs *resolution) gatherVariables(ctx context.Context) error {
	// reset variable and queue
	rs.variables = make(map[deppy.Identifier]*Variable)

	// nil variable signals to variable sources that only create variables to start creating
	rs.variableQueue = []*Variable{nil}

	var curVar *Variable
	for len(rs.variableQueue) > 0 {
		curVar, rs.variableQueue = rs.variableQueue[0], rs.variableQueue[1:]
		for _, source := range rs.sources {
			err := source.Update(ctx, rs, curVar)
			if IsFatalError(err) {
				return err
			}
			// todo: add warning log
		}
	}
	return nil
}

func (rs *resolution) Peek() *Variable {
	return nil
}
