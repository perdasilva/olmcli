package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/operator-framework/deppy/pkg/deppy/solver"
	"github.com/perdasilva/olmcli/internal/resolver"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/sirupsen/logrus"
)

var logger = logrus.Logger{
	Out:   os.Stderr,
	Level: logrus.InfoLevel,
	Formatter: &logrus.TextFormatter{
		ForceColors:     true,
		DisableColors:   false,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	},
}

func main() {

	DB, err := store.NewPackageDatabase(path.Join("/home/perdasilva/.olm", "olm.db"), &logger)
	if err != nil {
		panic(err)
	}

	requiredPackageSource := resolver.NewDynamicSource(
		"required-package",
		`
type: VariableEdit
action: Add
variableId: "required-package/kuadrant-operator"
kind: olm.variable.required-package
params:
  properties:
    "olm.package.name": "kuadrant-operator"
    "olm.package.version": ">0.0.0"`,
		"",
		DB,
	)

	requiredPackageMandatorySource := resolver.NewDynamicSource(
		"required-package-mandatory",
		`
type: ConstraintEdit
action: Add
kind: olm.variable.required-package
variableId: "{{ .Var.VariableID }}"
constraintType: Mandatory`,
		"olm.variable.required-package",
		DB,
	)

	requiredPackageBundlesSource := resolver.NewDynamicSource(
		"required-package-bundles",
		`
{{ range getBundlesForPackage (.Var.Property "olm.package.name") (.Var.Property "olm.package.version") (.Var.Property "olm.package.channel") }}
---
type: VariableEdit
action: Upsert
variableId: olm-bundle/{{ .BundleID }}
kind: olm.variable.bundle
params:
  properties:
    "olm.package.name": "{{ .PackageName }}"
    "olm.package.version": "{{ .Version }}"
    "olm.bundle.id": "{{ .BundleID }}"
    "olm.bundle.image": "{{ .BundlePath }}"
    "olm.package.required": {{ .PackageDependenciesJSON }}
    "olm.gvk.required": {{ .RequiredAPIsJSON }}
    "olm.gvk.provided": {{ .ProvidedAPIsJSON }}
{{ end }}`,
		"olm.variable.required-package",
		DB,
	)

	requiredPackageDependencySource := resolver.NewDynamicSource(
		"required-package-dependency",
		`
type: ConstraintEdit
action: Add
variableId: required-package/{{ .Var.Property "olm.package.name" }}
kind: olm.variable.bundle
constraintType: Dependency
constraintId: required-package/{{ .Var.Property "olm.package.name" }}
params:
  dependentVariableId: {{ .Var.VariableID }}`,
		"olm.variable.bundle",
		DB,
	)

	bundlePkgDependencySource := resolver.NewDynamicSource(
		"bundle-dependency",
		`
{{ with . }} {{$ := .}}
{{ range getPackageDependencies (.Var.Property "olm.bundle.id") }} {{$outer := .}}
{{ range getBundlesForPackage ($outer.PackageName) ($outer.Version) "" }}
---
type: VariableEdit
action: Upsert
variableId: olm-bundle/{{ .BundleID }}
kind: olm.variable.bundle
params:
  properties:
    "olm.package.name": "{{ .PackageName }}"
    "olm.package.version": "{{ .Version }}"
    "olm.bundle.id": "{{ .BundleID }}"
    "olm.bundle.image": "{{ .BundlePath }}"
    "olm.package.required": {{ .PackageDependenciesJSON }}
    "olm.gvk.required": {{ .RequiredAPIsJSON }}
    "olm.gvk.provided": {{ .ProvidedAPIsJSON }}
---
type: ConstraintEdit
action: Add
variableId: {{ $.Var.VariableID }}
kind: olm.variable.bundle
constraintId: required-package/{{ .PackageName }}
constraintType: Dependency
params:
  dependentVariableId: olm-bundle/{{ .BundleID }}
{{ end }}
{{ end }}
{{ end }}`,
		"olm.variable.bundle",
		DB,
	)

	bundleGVKDependencySource := resolver.NewDynamicSource(
		"gvk-dependency",
		`
{{ with . }} {{$ := .}}
{{ range .Var.Property "olm.gvk.required" }} {{$outer := .}}
{{ range getBundlesForGVK ($outer.group) ($outer.version) ($outer.kind) }}
---
type: VariableEdit
action: Upsert
variableId: olm-bundle/{{ .BundleID }}
kind: olm.variable.bundle
params:
  properties:
    "olm.package.name": "{{ .PackageName }}"
    "olm.package.version": "{{ .Version }}"
    "olm.bundle.id": "{{ .BundleID }}"
    "olm.bundle.image": "{{ .BundlePath }}"
    "olm.package.required": {{ .PackageDependenciesJSON }}
    "olm.gvk.required": {{ .RequiredAPIsJSON }}
    "olm.gvk.provided": {{ .ProvidedAPIsJSON }}
---
type: ConstraintEdit
action: Add
variableId: {{ $.Var.VariableID }}
kind: olm.variable.bundle
constraintId: required-gvk/{{ $outer.group }}:{{ $outer.version }}:{{ $outer.kind }}
constraintType: Dependency
params:
  dependentVariableId: olm-bundle/{{ .BundleID }}
{{ end }}
{{ end }}
{{ end }}`,
		"olm.variable.bundle",
		DB,
	)

	packageUniquenessSource := resolver.NewDynamicSource(
		"package-uniqueness",
		`
type: VariableEdit
action: Add
variableId: "uniqueness/package-uniqueness"
kind: olm.variable.uniqueness
`,
		"",
		DB,
	)

	gvkUniquenessSource := resolver.NewDynamicSource(
		"gvk-uniqueness",
		`
type: VariableEdit
action: Add
variableId: "uniqueness/gvk-uniqueness"
kind: olm.variable.uniqueness`,
		"",
		DB,
	)

	packageUniquenessDependencySource := resolver.NewDynamicSource(
		"package-uniqueness-dependency",
		`
type: ConstraintEdit
action: Add
variableId: uniqueness/package-uniqueness
kind: olm.variable.uniqueness
constraintType: AtMost
constraintId: package-uniqueness/{{ .Var.Property "olm.package.name" }}
params:
  n: 1
  variableId: {{ $.Var.VariableID }}`,
		"olm.variable.bundle",
		DB,
	)

	gvkUniquenessDependencySource := resolver.NewDynamicSource(
		"gvk-uniqueness-dependency",
		`
{{ with . }} {{$ := .}}
{{ range .Var.Property "olm.gvk.provided" }} {{$outer := .}}
---
type: ConstraintEdit
action: Add
variableId: uniqueness/gvk-uniqueness
kind: olm.variable.uniqueness
constraintType: AtMost
constraintId: gvk-uniqueness/{{ $outer.group }}:{{ $outer.version }}:{{ $outer.kind }}
params:
  n: 1
  variableId: {{ $.Var.VariableID }}
{{ end }}
{{ end }}`,
		"olm.variable.bundle",
		DB,
	)

	variableSources := []resolver.VariableSource{
		requiredPackageSource,
		requiredPackageMandatorySource,
		requiredPackageBundlesSource,
		requiredPackageDependencySource,
		bundlePkgDependencySource,
		bundleGVKDependencySource,
		gvkUniquenessSource,
		packageUniquenessSource,
		packageUniquenessDependencySource,
		gvkUniquenessDependencySource,
	}

	resolution := resolver.NewResolution(variableSources...)
	ctx := context.Background()

	// variables, err := resolution.GetVariables(ctx, nil)
	s, _ := solver.NewDeppySolver(nil, resolution)
	solution, err := s.Solve(ctx, solver.AddAllVariablesToSolution())
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range solution.SelectedVariables() {
		j, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(j))
	}
}
