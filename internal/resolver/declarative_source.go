package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/blang/semver/v4"
	"github.com/ghodss/yaml"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/perdasilva/olmcli/internal/store"
)

const (
	TaskTypeEdits    = "EditTask"
	TaskTypeTemplate = "Template"
	DoTypeEdits      = "DoEdits"
	DoTypeFor        = "DoFor"
)

func FromDir(path string, db store.PackageDatabase) ([]VariableSource, error) {
	var output []VariableSource
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".yaml" {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			docs := strings.Split(string(data), "---")
			for _, doc := range docs {
				if doc == "" {
					continue
				}
				doc = strings.Trim(doc, "\n")
				jsonData, err := yaml.YAMLToJSON([]byte(doc))
				if err != nil {
					return err
				}

				var source DeclarativeVariableSource
				err = json.Unmarshal(jsonData, &source)
				if err != nil {
					return err
				}

				source.Init(db)
				filteredSource := &FilteredVariableSource{
					VariableSource: &source,
					filter: func(variable *Variable) bool {
						if source.Filter == "" {
							return variable == nil
						} else {
							return variable != nil && variable.Kind() == source.Filter
						}
					},
					seen: map[string]*struct{}{},
				}
				output = append(output, filteredSource)
			}
		}
		return nil
	})
	return output, err
}

var _ VariableSource = &DeclarativeVariableSource{}

type DeclarativeVariableSource struct {
	SourceID string `json:"id"`
	// todo: could also be a template
	Filter   string `json:"kindFilter"`
	Task     Task   `json:"task"`
	database store.PackageDatabase
}

func NewDeclarativeVariableSource(sourceID string, filter string, task Task, database store.PackageDatabase) VariableSource {
	return &FilteredVariableSource{
		VariableSource: &DeclarativeVariableSource{
			SourceID: sourceID,
			Filter:   filter,
			Task:     task,
			database: database,
		},
		filter: func(variable *Variable) bool {
			if filter == "" {
				return variable == nil
			} else {
				return variable != nil && variable.Kind() == filter
			}
		},
		seen: map[string]*struct{}{},
	}
}

func (d *DeclarativeVariableSource) VariableSourceID() string {
	return d.SourceID
}

func (d *DeclarativeVariableSource) Init(database store.PackageDatabase) {
	if database == nil {
		panic("database cannot be nil")
	}
	d.database = database
}

func (d *DeclarativeVariableSource) Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error {
	edits, err := d.collectEdits(ctx, nextVariable)
	if err != nil {
		return err
	}
	for _, edit := range edits {
		if err := edit.Execute(resolution); err != nil {
			return err
		}
	}
	return nil
}

func (d *DeclarativeVariableSource) collectEdits(ctx context.Context, variable *Variable) ([]Edit, error) {
	exprEnv := map[string]interface{}{
		"curVar": variable,
		"repo":   d.database,
		"ctx":    ctx,

		// todo: move to straight up using the repo instead of these wrappers
		"getBundlesForPackage": func(packageName string, version string, channel string) ([]store.CachedBundle, error) {
			var opts []store.PackageSearchOption
			if version != "" {
				versionRange, err := semver.ParseRange(version)
				if err != nil {
					return nil, err
				}
				opts = append(opts, store.InVersionRange(versionRange))
			}
			if channel != "" {
				opts = append(opts, store.InChannel(channel))
			}
			return d.database.GetBundlesForPackage(context.Background(), packageName, opts...)
		},
		"getPackageDependencies": func(bundleId string) ([]property.Package, error) {
			bundle, err := d.database.GetBundle(context.Background(), bundleId)
			if err != nil {
				return nil, err
			}
			return bundle.PackageDependencies, nil
		},
		"getBundlesForGVK": func(group string, version string, kind string) ([]store.CachedBundle, error) {
			return d.database.GetBundlesForGVK(context.Background(), group, version, kind)
		},
		"getUpgradeBundles": func(bundleId string) ([]store.CachedBundle, error) {
			oldBundle, err := d.database.GetBundle(context.Background(), bundleId)
			if err != nil {
				return nil, err
			}
			vRange, err := semver.ParseRange(fmt.Sprintf(">= %s", oldBundle.Version))
			if err != nil {
				return nil, err
			}
			bundles, err := d.database.GetBundlesForPackage(context.Background(), oldBundle.PackageName, store.InChannel(oldBundle.ChannelName), store.InVersionRange(vRange))
			if err != nil {
				return nil, err
			}

			// Sort bundles by version descending
			sort.Slice(bundles, func(i, j int) bool {
				return semver.MustParse(bundles[i].Version).GT(semver.MustParse(bundles[j].Version))
			})

			out := make([]store.CachedBundle, 0, len(bundles))
			out = append(out, *oldBundle) // you can always upgrade to yourself
			for i := 0; i < len(bundles); i++ {
				if bundles[i].Replaces == oldBundle.CsvName {
					out = append(out, bundles[i])
				}
			}
			return out, nil
		},
		"rangeStr": func(start, end int) []string {
			out := make([]string, end-start)
			for i := start; i < end; i++ {
				out[i-start] = fmt.Sprintf("%d", i)
			}
			return out
		},
		"string": func(s interface{}) string {
			return fmt.Sprintf("%s", s)
		},
	}

	if d.Task.TaskType == TaskTypeEdits {
		return d.Task.EditTask.collectEdits(exprEnv)
	} else if d.Task.TaskType == TaskTypeTemplate {
		return d.Task.TemplateTask.collectEdits(exprEnv)
	}

	return nil, FatalError(fmt.Sprintf("unknown task type '%s'", d.Task.TaskType))
}

type Task struct {
	TaskType     string        `json:"taskType"`
	EditTask     *EditTask     `json:"editTask,omitempty"`
	TemplateTask *TemplateTask `json:"templateTask,omitempty"`
}

type EditTask struct {
	Edits         []Edit `json:"edits,omitempty"`
	compiledCache map[string]*vm.Program
}

func (et *EditTask) collectEdits(exprEnv map[string]interface{}) ([]Edit, error) {
	var edits []Edit
	for _, edit := range et.Edits {
		editType, err := convertTo[string](et.applyTemplate(edit.Type, exprEnv))
		if err != nil {
			return nil, err
		}
		action, err := convertTo[string](et.applyTemplate(edit.Action, exprEnv))
		if err != nil {
			return nil, err
		}
		variableID, err := convertTo[string](et.applyTemplate(edit.VariableID, exprEnv))
		if err != nil {
			return nil, err
		}
		kind, err := convertTo[string](et.applyTemplate(edit.Kind, exprEnv))
		if err != nil {
			return nil, err
		}
		constraintID, err := convertTo[string](et.applyTemplate(edit.ConstraintID, exprEnv))
		if err != nil {
			return nil, err
		}
		constraintType, err := convertTo[string](et.applyTemplate(edit.ConstraintType, exprEnv))
		if err != nil {
			return nil, err
		}
		params, err := convertTo[map[string]interface{}](et.applyTemplate(edit.Params, exprEnv))
		if err != nil {
			return nil, err
		}

		edit := Edit{
			Type:           editType,
			Action:         action,
			VariableID:     variableID,
			Kind:           kind,
			ConstraintID:   constraintID,
			ConstraintType: constraintType,
			Params:         params,
		}

		if err := edit.validate(); err != nil {
			return nil, err
		}

		edits = append(edits, edit)
	}
	et.compiledCache = nil
	return edits, nil
}

func (et *EditTask) applyTemplate(input interface{}, env map[string]interface{}) (interface{}, error) {
	if input == nil {
		return nil, nil
	}
	switch v := input.(type) {
	case string:
		return et.applyTemplateToString(v, env)
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, e := range v {
			var err error
			out[i], err = et.applyTemplate(e, env)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]interface{})
		for k, v := range v {
			key, err := convertTo[string](et.applyTemplateToString(k, env))
			if err != nil {
				return nil, err
			}
			value, err := et.applyTemplate(v, env)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
		return out, nil
	default:
		return input, nil
	}
}

func (et *EditTask) runExpression(expression string, env map[string]interface{}) (interface{}, error) {
	expression = strings.Trim(expression, "{{}} ")
	if et.compiledCache == nil {
		et.compiledCache = make(map[string]*vm.Program)
	}

	_, ok := et.compiledCache[expression]
	if !ok {
		program, err := expr.Compile(expression)
		if err != nil {
			return nil, FatalError(fmt.Sprintf("failed to compile template: %s", err))
		}
		et.compiledCache[expression] = program
	}
	return expr.Run(et.compiledCache[expression], env)
}

func (et *EditTask) applyTemplateToString(input string, env map[string]interface{}) (interface{}, error) {
	re := regexp.MustCompile(`^\{\{[^{}]*\}\}$`)
	if re.MatchString(input) {
		return et.runExpression(input, env)
	}

	re = regexp.MustCompile(`(?s){{.*?}}`)
	var outerErr error
	out := re.ReplaceAllStringFunc(input, func(match string) string {
		out, err := et.runExpression(match, env)
		if err != nil {
			outerErr = err
			return ""
		}
		if out == nil {
			outerErr = err
			return ""
		}
		switch o := out.(type) {
		case string:
			return fmt.Sprintf("%s", o)
		case int:
			return fmt.Sprintf("%d", o)
		default:
			outerErr = FatalError(fmt.Sprintf("template must return string, got %T", o))
			return ""
		}
	})
	if outerErr != nil {
		return "", outerErr
	}
	return out, nil
}

type TemplateTask struct {
	Template Template `json:"template,omitempty"`
}

func (tt TemplateTask) collectEdits(exprEnv map[string]interface{}) ([]Edit, error) {
	return tt.Template.collectEdits(exprEnv)
}

type Template struct {
	ForLoop ForLoop `json:"for"`
}

func (t Template) collectEdits(exprEnv map[string]interface{}) ([]Edit, error) {
	return t.ForLoop.collectEdits(exprEnv)
}

type ForLoop struct {
	Variable      string `json:"variable"`
	Query         string `json:"query"`
	Do            Do     `json:"do"`
	compiledQuery *vm.Program
}

func (f *ForLoop) collectEdits(exprEnv map[string]interface{}) ([]Edit, error) {
	defer delete(exprEnv, f.Variable)

	if _, ok := exprEnv[f.Variable]; ok {
		return nil, FatalError(fmt.Sprintf("variable %s already exists in template environment", f.Variable))
	}
	if f.compiledQuery == nil {
		query := strings.Trim(f.Query, "{{}} ")
		program, err := expr.Compile(query)
		if err != nil {
			return nil, FatalError(fmt.Sprintf("failed to compile query: %s", err))
		}
		f.compiledQuery = program
	}

	results, err := expr.Run(f.compiledQuery, exprEnv)
	if err != nil {
		return nil, FatalError(fmt.Sprintf("failed to execute query: %s", err))
	}

	var s []interface{}
	v := reflect.ValueOf(results)
	if v.Kind() == reflect.Slice {
		s = make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			s[i] = v.Index(i).Interface()
		}
	} else if results != nil {
		s = []interface{}{results}
	}

	var edits []Edit
	for _, result := range s {
		exprEnv[f.Variable] = result
		newEdits, err := f.Do.collectEdits(exprEnv)
		if err != nil {
			return nil, err
		}
		edits = append(edits, newEdits...)
	}
	return edits, nil
}

type Do struct {
	DoType   string    `json:"doType"`
	EditTask *EditTask `json:"edits,omitempty"`
	ForLoop  *ForLoop  `json:"for,omitempty"`
}

func (d *Do) collectEdits(exprEnv map[string]interface{}) ([]Edit, error) {
	if d.DoType == DoTypeEdits {
		return d.EditTask.collectEdits(exprEnv)
	} else if d.DoType == DoTypeFor {
		if d.ForLoop == nil {
			return nil, FatalError("for loop cannot be nil")
		}
		return d.ForLoop.collectEdits(exprEnv)
	}

	return nil, FatalError(fmt.Sprintf("unknown do type %s", d.DoType))
}

func convertTo[T any](input interface{}, err error) (T, error) {
	if err != nil {
		return *new(T), err
	}
	if input == nil {
		return *new(T), nil
	}
	out, ok := input.(T)
	if !ok {
		return *new(T), FatalError(fmt.Sprintf("expected type %T, got %T", out, input))
	}
	return out, nil
}
