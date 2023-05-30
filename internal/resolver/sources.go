package resolver

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/operator-framework/operator-registry/pkg/api"
	"github.com/perdasilva/olmcli/internal/store"
)

func NewOLMVariableSources(database store.PackageDatabase) []VariableSource {
	return []VariableSource{
		NewRequiredPackageDependencySource(),
		NewRequiredPackageBundlesSource(database),
		NewBundlePackageDependenciesSource(database),
		NewBundleGVKDependenciesSource(database),
		NewPackageUniquenessConstraintsSource(),
		NewGVKUniquenessConstraintsSource(),
	}
}

var _ VariableSource = &FilteredVariableSource{}

type VariableFilter func(variable *Variable) bool

type FilteredVariableSource struct {
	VariableSource
	filter VariableFilter
	seen   map[string]*struct{}
}

func (f FilteredVariableSource) Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error {
	varId := "nil" // nil variable is acceptable to kick off variable creation by sources
	if nextVariable != nil {
		varId = nextVariable.Identifier().String()
	}

	if !f.filter(nextVariable) || f.seen[varId] != nil {
		return nil
	}

	err := f.VariableSource.Update(ctx, resolution, nextVariable)
	if err == nil {
		f.seen[varId] = &struct{}{}
	}
	return err
}

var _ VariableSource = &RequiredPackageSource{}

type RequiredPackageSource struct {
	packageName  string
	versionRange string
}

func NewRequiredPackageSource(packageName string, versionRange string) VariableSource {
	return FilteredVariableSource{
		VariableSource: &RequiredPackageSource{
			packageName:  packageName,
			versionRange: versionRange,
		},
		filter: func(variable *Variable) bool {
			return variable == nil
		},
		seen: map[string]*struct{}{},
	}
}

func (r RequiredPackageSource) VariableSourceID() string {
	return fmt.Sprintf("required-package/%s", r.packageName)
}

func (r RequiredPackageSource) Update(_ context.Context, resolution Resolution, _ *Variable) error {
	variable, err := resolution.NewVariable(r.VariableSourceID(), "olm.variable.required-package", map[string]interface{}{
		"olm.package.name":    r.packageName,
		"olm.package.version": r.versionRange,
	})
	variable.AddMandatory()
	return err
}

var _ VariableSource = RequiredPackageBundlesSource{}

type RequiredPackageBundlesSource struct {
	packageDB store.PackageDatabase
}

func NewRequiredPackageBundlesSource(packageDB store.PackageDatabase) VariableSource {
	return FilteredVariableSource{
		VariableSource: RequiredPackageBundlesSource{
			packageDB: packageDB,
		},
		filter: func(variable *Variable) bool {
			return variable != nil && variable.Kind() == "olm.variable.required-package"
		},
		seen: map[string]*struct{}{},
	}
}

func (r RequiredPackageBundlesSource) VariableSourceID() string {
	return "required-package-bundles"
}

func (r RequiredPackageBundlesSource) Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error {
	packageName := nextVariable.Properties["olm.package.name"].(string)
	versionRange := semver.MustParseRange(nextVariable.Properties["olm.package.version"].(string))

	bundles, err := r.packageDB.GetBundlesForPackage(ctx, packageName, store.InVersionRange(versionRange))
	if err != nil {
		return err
	}

	for _, bundle := range bundles {
		variableId := fmt.Sprintf("olm-bundle/%s", bundle.BundleID)
		properties := map[string]interface{}{
			"olm.package.name":    bundle.PackageName,
			"olm.package.version": bundle.Version,
		}
		if len(bundle.PackageDependencies) > 0 {
			properties["olm.package.required"] = bundle.PackageDependencies
		}
		if len(bundle.RequiredApis) > 0 {
			properties["olm.gvk.required"] = bundle.RequiredApis
		}
		if len(bundle.ProvidedApis) > 0 {
			properties["olm.gvk.provided"] = bundle.ProvidedApis
		}
		_, err := resolution.UpsertVariable(variableId, "olm.variable.bundle", properties)

		if err != nil {
			return err
		}
	}

	return nil
}

var _ VariableSource = &RequiredPackageDependencySource{}

type RequiredPackageDependencySource struct {
}

func NewRequiredPackageDependencySource() VariableSource {
	return FilteredVariableSource{
		VariableSource: RequiredPackageDependencySource{},
		filter: func(variable *Variable) bool {
			return variable != nil && variable.Kind() == "olm.variable.bundle"
		},
		seen: map[string]*struct{}{},
	}
}

func (r RequiredPackageDependencySource) VariableSourceID() string {
	return "required-package-dependency"
}

func (r RequiredPackageDependencySource) Update(_ context.Context, resolution Resolution, nextVariable *Variable) error {
	variable, err := resolution.GetVariable(fmt.Sprintf("required-package/%s", nextVariable.Properties["olm.package.name"].(string)))
	if err != nil {
		return IgnoreNotFound(err)
	}
	dependencyId := fmt.Sprintf("required-package/%s", nextVariable.Properties["olm.package.name"].(string))
	return variable.AddDependency(dependencyId, *nextVariable)
}

var _ VariableSource = BundlePackageDependenciesSource{}

type BundlePackageDependenciesSource struct {
	packageDB store.PackageDatabase
}

func NewBundlePackageDependenciesSource(packageDB store.PackageDatabase) VariableSource {
	return FilteredVariableSource{
		VariableSource: BundlePackageDependenciesSource{
			packageDB: packageDB,
		},
		filter: func(variable *Variable) bool {
			return variable != nil && variable.Kind() == "olm.variable.bundle"
		},
		seen: map[string]*struct{}{},
	}
}

func (r BundlePackageDependenciesSource) VariableSourceID() string {
	return "bundle-package-dependencies"
}

func (r BundlePackageDependenciesSource) Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error {
	pkgDependencies, ok := nextVariable.Properties["olm.package.required"]
	if ok {
		switch d := pkgDependencies.(type) {
		case []property.Package:
			for _, pkgDep := range d {
				packageName := pkgDep.PackageName
				versionRange := semver.MustParseRange(pkgDep.Version)
				bundles, err := r.packageDB.GetBundlesForPackage(ctx, packageName, store.InVersionRange(versionRange))
				if err != nil {
					return err
				}
				depId := fmt.Sprintf("required-package/%s", packageName)
				for _, bundle := range bundles {
					variableId := fmt.Sprintf("olm-bundle/%s", bundle.BundleID)
					properties := map[string]interface{}{
						"olm.package.name":    bundle.PackageName,
						"olm.package.version": bundle.Version,
					}
					if len(bundle.PackageDependencies) > 0 {
						properties["olm.package.required"] = bundle.PackageDependencies
					}
					if len(bundle.RequiredApis) > 0 {
						properties["olm.gvk.required"] = bundle.RequiredApis
					}
					if len(bundle.ProvidedApis) > 0 {
						properties["olm.gvk.provided"] = bundle.ProvidedApis
					}

					variable, err := resolution.UpsertVariable(variableId, "olm.variable.bundle", properties)
					if err != nil {
						return err
					}
					if err := nextVariable.AddDependency(depId, *variable); err != nil {
						return err
					}
				}
			}
		default:
			return PreconditionError(fmt.Sprintf("olm.variable.bundle: olm.package.required must be a slice of property.Package, got %T", pkgDependencies))
		}
	}
	return nil
}

var _ VariableSource = BundleGVKDependenciesSource{}

type BundleGVKDependenciesSource struct {
	packageDB store.PackageDatabase
}

func NewBundleGVKDependenciesSource(packageDB store.PackageDatabase) VariableSource {
	return FilteredVariableSource{
		VariableSource: BundleGVKDependenciesSource{
			packageDB: packageDB,
		},
		filter: func(variable *Variable) bool {
			return variable != nil && variable.Kind() == "olm.variable.bundle"
		},
		seen: map[string]*struct{}{},
	}
}

func (r BundleGVKDependenciesSource) VariableSourceID() string {
	return "bundle-gvk-dependencies"
}

func (r BundleGVKDependenciesSource) Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error {
	gvkDependencies, ok := nextVariable.Properties["olm.gvk.required"]
	if ok {
		switch d := gvkDependencies.(type) {
		case []*api.GroupVersionKind:
			for _, gvkDep := range d {
				bundles, err := r.packageDB.GetBundlesForGVK(ctx, gvkDep.Group, gvkDep.Version, gvkDep.Kind)
				if err != nil {
					return err
				}
				depId := fmt.Sprintf("required-gvk/%s:%s:%s", gvkDep.Group, gvkDep.Version, gvkDep.Kind)
				for _, bundle := range bundles {
					variableId := fmt.Sprintf("olm-bundle/%s", bundle.BundleID)
					variable, err := resolution.UpsertVariable(variableId, "olm.variable.bundle", map[string]interface{}{
						"olm.package.name":     bundle.PackageName,
						"olm.package.version":  bundle.Version,
						"olm.package.required": bundle.PackageDependencies,
						"olm.gvk.required":     bundle.RequiredApis,
						"olm.gvk.provided":     bundle.ProvidedApis,
					})

					if err != nil {
						return err
					}
					if err := nextVariable.AddDependency(depId, *variable); err != nil {
						return err
					}
				}
			}
		default:
			return PreconditionError(fmt.Sprintf("olm.variable.bundle: olm.gvk.required must be a slice of api.GroupVersionKind, got %T", gvkDependencies))
		}
	}
	return nil
}

var _ VariableSource = &PackageUniquenessConstraintsSource{}

type PackageUniquenessConstraintsSource struct {
}

func NewPackageUniquenessConstraintsSource() VariableSource {
	return FilteredVariableSource{
		VariableSource: PackageUniquenessConstraintsSource{},
		filter: func(variable *Variable) bool {
			return variable != nil && variable.Kind() == "olm.variable.bundle"
		},
		seen: map[string]*struct{}{},
	}
}

func (u PackageUniquenessConstraintsSource) VariableSourceID() string {
	return "package-uniqueness-constraints"
}

func (u PackageUniquenessConstraintsSource) Update(_ context.Context, resolution Resolution, nextVariable *Variable) error {
	packageName := nextVariable.Properties["olm.package.name"].(string)
	if packageName == "" {
		return PreconditionError("olm.variable.bundle: olm.package.name must be set")
	}

	varId := fmt.Sprintf("uniqueness-constraints")
	variable, err := resolution.UpsertVariable(varId, "olm.variable.uniqueness-constraints", nil)
	if err != nil {
		return err
	}
	depId := fmt.Sprintf("package-uniqueness/%s", packageName)
	return variable.AddAtMost(depId, 1, *nextVariable)
}

var _ VariableSource = &GVKUniquenessConstraintsSource{}

type GVKUniquenessConstraintsSource struct {
}

func NewGVKUniquenessConstraintsSource() VariableSource {
	return FilteredVariableSource{
		VariableSource: GVKUniquenessConstraintsSource{},
		filter: func(variable *Variable) bool {
			return variable != nil && variable.Kind() == "olm.variable.bundle"
		},
		seen: map[string]*struct{}{},
	}
}

func (u GVKUniquenessConstraintsSource) VariableSourceID() string {
	return "gvk-uniqueness-constraints"
}

func (u GVKUniquenessConstraintsSource) Update(ctx context.Context, resolution Resolution, nextVariable *Variable) error {
	_, ok := nextVariable.Properties["olm.gvk.provided"]
	if !ok {
		return nil
	}

	providedGVKs, ok := nextVariable.Properties["olm.gvk.provided"].([]*api.GroupVersionKind)
	if !ok {
		return PreconditionError(fmt.Sprintf("olm.variable.bundle: olm.gvk.provided must be a slice of *api.GroupVersionKind, got %T", providedGVKs))
	}

	if len(providedGVKs) > 0 {
		varId := fmt.Sprintf("uniqueness-constraints")
		variable, err := resolution.UpsertVariable(varId, "olm.variable.uniqueness-constraints", nil)
		if err != nil {
			return err
		}
		for _, gvk := range providedGVKs {
			depId := fmt.Sprintf("gvk-uniqueness/%s:%s:%s", gvk.Group, gvk.Version, gvk.Kind)
			if err := variable.AddAtMost(depId, 1, *nextVariable); err != nil {
				return err
			}
		}
	}
	return nil
}
