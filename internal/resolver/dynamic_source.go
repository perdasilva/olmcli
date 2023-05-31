package resolver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/perdasilva/olmcli/internal/store"
	"gopkg.in/yaml.v3"
)

var _ VariableSource = &DynamicSource{}

type DynamicSource struct {
	ID         string `yaml:"variableSourceId"`
	Template   string `yaml:"template"`
	KindFilter string `yaml:"kindFilter,omitempty"`
	repo       store.PackageDatabase
	t          *template.Template
}

func NewDynamicSource(id string, template string, kindFilter string, repo store.PackageDatabase) VariableSource {
	return FilteredVariableSource{
		VariableSource: &DynamicSource{
			ID:       id,
			Template: template,
			repo:     repo,
		},
		filter: func(variable *Variable) bool {
			if kindFilter == "" {
				return variable == nil
			} else {
				return variable != nil && variable.Kind() == kindFilter
			}
		},
		seen: make(map[string]*struct{}),
	}
}

func (d *DynamicSource) VariableSourceID() string {
	return d.ID
}

func (d *DynamicSource) Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error {
	if err := d.lazyLoadTemplate(); err != nil {
		return err
	}
	templateEnvironment := &struct {
		Resolution Resolution
		Var        *Variable
		Repo       store.PackageDatabase
		Ctx        context.Context
	}{
		Resolution: resolution,
		Var:        nextVariable,
		Repo:       d.repo,
		Ctx:        ctx,
	}

	var buf bytes.Buffer
	if err := d.t.Execute(&buf, templateEnvironment); err != nil {
		return FatalError(err.Error())
	}

	var edits []Edit

	re := regexp.MustCompile(`(?m)^\s*$[\r\n]*|[\r\n]+\s+\z`)
	s := re.ReplaceAllString(buf.String(), "")

	decoder := yaml.NewDecoder(strings.NewReader(s))
	for {
		var edit Edit
		err := decoder.Decode(&edit)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return FatalError(err.Error())
		}
		edits = append(edits, edit)
	}

	sort.Slice(edits, func(i, j int) bool {
		if edits[i].Type == edits[j].Type {
			return false
		}
		return edits[i].Type == EditTypeVariable
	})

	for _, edit := range edits {
		if err := edit.Execute(resolution); err != nil {
			return err
		}
	}

	return nil
}

func (d *DynamicSource) lazyLoadTemplate() error {
	if d.t != nil {
		return nil
	}

	t, err := template.New(d.ID).Funcs(template.FuncMap{
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
			return d.repo.GetBundlesForPackage(context.Background(), packageName, opts...)
		},
		"getPackageDependencies": func(bundleId string) ([]property.Package, error) {
			bundle, err := d.repo.GetBundle(context.Background(), bundleId)
			if err != nil {
				return nil, err
			}
			return bundle.PackageDependencies, nil
		},
		"getBundlesForGVK": func(group string, version string, kind string) ([]store.CachedBundle, error) {
			return d.repo.GetBundlesForGVK(context.Background(), group, version, kind)
		},
	}).Parse(d.Template)
	if err != nil {
		return FatalError(err.Error())
	}
	d.t = t
	return nil
}
