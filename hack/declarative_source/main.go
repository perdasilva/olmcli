package main

import (
	"fmt"
	"os"
	"path"

	"github.com/ghodss/yaml"
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

	requiredPackageSource := resolver.Task{
		TaskType: resolver.TaskTypeEdits,
		EditTask: &resolver.EditTask{
			Edits: []resolver.Edit{
				{
					Type:       resolver.EditTypeVariable,
					Action:     resolver.ActionAdd,
					Kind:       "olm.variable.required-package",
					VariableID: "required-package/instana-agent-operator",
					Params: map[string]interface{}{
						"properties": map[string]interface{}{
							"olm.package.name":    "instana-agent-operator",
							"olm.package.version": "<=2.0.5",
						},
					},
				},
			},
		},
	}

	installedPackageSource := resolver.Task{
		TaskType: resolver.TaskTypeEdits,
		EditTask: &resolver.EditTask{
			Edits: []resolver.Edit{
				{
					Type:       resolver.EditTypeVariable,
					Action:     resolver.ActionAdd,
					Kind:       "olm.variable.installed-package",
					VariableID: "installed-package/instana-agent-operator",
					Params: map[string]interface{}{
						"properties": map[string]interface{}{
							"olm.package.name":    "instana-agent-operator",
							"olm.package.version": "2.0.1",
							"olm.bundle.id":       "catalog/instana-agent-operator/stable/instana-agent-operator.v2.0.1",
						},
					},
				},
			},
		},
	}

	installedPackageUpgradeSource := resolver.Task{
		TaskType: resolver.TaskTypeTemplate,
		TemplateTask: &resolver.TemplateTask{
			Template: resolver.Template{
				ForLoop: resolver.ForLoop{
					Variable: "bundle",
					Query:    `{{ getUpgradeBundles(curVar.Property("olm.bundle.id")) }}`,
					Do: resolver.Do{
						DoType: resolver.DoTypeEdits,
						EditTask: &resolver.EditTask{
							Edits: []resolver.Edit{
								{
									Type:       resolver.EditTypeVariable,
									Action:     resolver.ActionUpsert,
									Kind:       "olm.variable.bundle",
									VariableID: "olm-bundle/{{ bundle.BundleID }}",
									Params: map[string]interface{}{
										"properties": map[string]interface{}{
											"olm.package.name":           "{{ bundle.PackageName }}",
											"olm.package.version":        "{{ bundle.Version }}",
											"olm.package.channel":        "{{ bundle.ChannelName }}",
											"olm.package.defaultChannel": "{{ bundle.DefaultChannelName }}",
											"olm.bundle.id":              "{{ bundle.BundleID }}",
											"olm.bundle.image":           "{{ bundle.BundlePath }}",
											"olm.package.required":       "{{ bundle.PackageDependencies }}",
											"olm.gvk.required":           "{{ bundle.RequiredApis }}",
											"olm.gvk.provided":           "{{ bundle.ProvidedApis }}",
										},
									},
								},
								{
									Type:           resolver.EditTypeConstraint,
									Action:         resolver.ActionAdd,
									ConstraintID:   `package/upgrade/{{ bundle.PackageName }}`,
									ConstraintType: resolver.ConstraintTypeDependency,
									Kind:           `{{ curVar.Kind() }}`,
									VariableID:     `{{ curVar.VariableID }}`,
									Params: map[string]interface{}{
										"dependentVariableId": "olm-bundle/{{ bundle.BundleID }}",
										"orderPreference":     `semverCompare(v1.Properties["olm.package.version"], v2.Properties["olm.package.version"]) > 0`,
									},
								},
								{
									Type:           resolver.EditTypeConstraint,
									Action:         resolver.ActionAdd,
									Kind:           `{{ curVar.Kind() }}`,
									ConstraintType: resolver.ConstraintTypeMandatory,
									VariableID:     "installed-package/{{ bundle.PackageName }}",
								},
							},
						},
					},
				},
			},
		},
	}

	requiredPackageMandatorySource := resolver.Task{
		TaskType: resolver.TaskTypeEdits,
		EditTask: &resolver.EditTask{
			Edits: []resolver.Edit{
				{
					Type:           resolver.EditTypeConstraint,
					Action:         resolver.ActionAdd,
					Kind:           "olm.variable.required-package",
					VariableID:     "{{ curVar.VariableID }}",
					ConstraintType: "Mandatory",
				},
			},
		},
	}

	requiredPackageBundlesSource := resolver.Task{
		TaskType: resolver.TaskTypeTemplate,
		TemplateTask: &resolver.TemplateTask{
			Template: resolver.Template{
				ForLoop: resolver.ForLoop{
					Variable: "bundle",
					Query:    `{{ getBundlesForPackage(curVar.Property("olm.package.name"), curVar.Property("olm.package.version"), curVar.Property("olm.package.channel")) }}`,
					Do: resolver.Do{
						DoType: resolver.DoTypeEdits,
						EditTask: &resolver.EditTask{
							Edits: []resolver.Edit{
								{
									Type:       resolver.EditTypeVariable,
									Action:     resolver.ActionUpsert,
									Kind:       "olm.variable.bundle",
									VariableID: "olm-bundle/{{ bundle.BundleID }}",
									Params: map[string]interface{}{
										"properties": map[string]interface{}{
											"olm.package.name":           "{{ bundle.PackageName }}",
											"olm.package.version":        "{{ bundle.Version }}",
											"olm.package.channel":        "{{ bundle.ChannelName }}",
											"olm.package.defaultChannel": "{{ bundle.DefaultChannelName }}",
											"olm.bundle.id":              "{{ bundle.BundleID }}",
											"olm.bundle.image":           "{{ bundle.BundlePath }}",
											"olm.package.required":       "{{ bundle.PackageDependencies }}",
											"olm.gvk.required":           "{{ bundle.RequiredApis }}",
											"olm.gvk.provided":           "{{ bundle.ProvidedApis }}",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	requiredPackageDependencySource := resolver.Task{
		TaskType: resolver.TaskTypeEdits,
		EditTask: &resolver.EditTask{
			Edits: []resolver.Edit{
				{
					Type:           resolver.EditTypeConstraint,
					Action:         resolver.ActionAdd,
					Kind:           "olm.variable.required-package",
					VariableID:     `required-package/{{ curVar.Property("olm.package.name") }}`,
					ConstraintType: "Dependency",
					ConstraintID:   `required-package/{{ curVar.Property("olm.package.name") }}`,
					Params: map[string]interface{}{
						"dependentVariableId": "{{ curVar.VariableID }}",
						"orderPreference":     `semverCompare(v1.Properties["olm.package.version"], v2.Properties["olm.package.version"]) > 0`,
					},
				},
			},
		},
	}

	bundlePackageDependenciesSource := resolver.Task{
		TaskType: resolver.TaskTypeTemplate,
		TemplateTask: &resolver.TemplateTask{
			Template: resolver.Template{
				ForLoop: resolver.ForLoop{
					Variable: "packageDependency",
					Query:    `{{ curVar.Property("olm.package.required") }}`,
					Do: resolver.Do{
						DoType: resolver.DoTypeFor,
						ForLoop: &resolver.ForLoop{
							Variable: "bundle",
							Query:    `{{ getBundlesForPackage(packageDependency.PackageName, packageDependency.Version, "") }}`,
							Do: resolver.Do{
								DoType: resolver.DoTypeEdits,
								EditTask: &resolver.EditTask{
									Edits: []resolver.Edit{
										{
											Type:       resolver.EditTypeVariable,
											Action:     resolver.ActionUpsert,
											Kind:       "olm.variable.bundle",
											VariableID: "olm-bundle/{{ bundle.BundleID }}",
											Params: map[string]interface{}{
												"properties": map[string]interface{}{
													"olm.package.name":           "{{ bundle.PackageName }}",
													"olm.package.version":        "{{ bundle.Version }}",
													"olm.package.channel":        "{{ bundle.ChannelName }}",
													"olm.package.defaultChannel": "{{ bundle.DefaultChannelName }}",
													"olm.bundle.id":              "{{ bundle.BundleID }}",
													"olm.bundle.image":           "{{ bundle.BundlePath }}",
													"olm.package.required":       "{{ bundle.PackageDependencies }}",
													"olm.gvk.required":           "{{ bundle.RequiredApis }}",
													"olm.gvk.provided":           "{{ bundle.ProvidedApis }}",
												},
											},
										}, {
											Type:           resolver.EditTypeConstraint,
											Action:         resolver.ActionAdd,
											Kind:           "olm.variable.bundle",
											VariableID:     "{{ curVar.VariableID }}",
											ConstraintType: "Dependency",
											ConstraintID:   "required-package/{{ bundle.PackageName }}",
											Params: map[string]interface{}{
												"dependentVariableId": "olm-bundle/{{ bundle.BundleID }}",
												// "orderPreference":     `semverCompare(v1.Properties["olm.package.version"], v2.Properties["olm.package.version"]) > 0`,
												"orderPreference": `multiSort(weightedCompare([v1.Properties["olm.package.defaultChannel"]], v1.Properties["olm.package.channel"], v1.Properties["olm.package.channel"]), semverCompare(v1.Properties["olm.package.version"], v2.Properties["olm.package.version"])) > 0`,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	bundleGVKDependenciesSource := resolver.Task{
		TaskType: resolver.TaskTypeTemplate,
		TemplateTask: &resolver.TemplateTask{
			Template: resolver.Template{
				ForLoop: resolver.ForLoop{
					Variable: "gvk",
					Query:    `{{ curVar.Property("olm.gvk.required") }}`,
					Do: resolver.Do{
						DoType: resolver.DoTypeFor,
						ForLoop: &resolver.ForLoop{
							Variable: "bundle",
							Query:    `{{ getBundlesForGVK(gvk.Group, gvk.Version, gvk.Kind) }}`,
							Do: resolver.Do{
								DoType: resolver.DoTypeEdits,
								EditTask: &resolver.EditTask{
									Edits: []resolver.Edit{
										{
											Type:       resolver.EditTypeVariable,
											Action:     resolver.ActionUpsert,
											Kind:       "olm.variable.bundle",
											VariableID: "olm-bundle/{{ bundle.BundleID }}",
											Params: map[string]interface{}{
												"properties": map[string]interface{}{
													"olm.package.name":           "{{ bundle.PackageName }}",
													"olm.package.version":        "{{ bundle.Version }}",
													"olm.package.channel":        "{{ bundle.ChannelName }}",
													"olm.package.defaultChannel": "{{ bundle.DefaultChannelName }}",
													"olm.bundle.id":              "{{ bundle.BundleID }}",
													"olm.bundle.image":           "{{ bundle.BundlePath }}",
													"olm.package.required":       "{{ bundle.PackageDependencies }}",
													"olm.gvk.required":           "{{ bundle.RequiredApis }}",
													"olm.gvk.provided":           "{{ bundle.ProvidedApis }}",
												},
											},
										}, {
											Type:           resolver.EditTypeConstraint,
											Action:         resolver.ActionAdd,
											Kind:           "olm.variable.bundle",
											VariableID:     "{{ curVar.VariableID }}",
											ConstraintType: "Dependency",
											ConstraintID:   "required-gvk/{{ gvk.Group }}:{{ gvk.Version }}:{{ gvk.Kind }}",
											Params: map[string]interface{}{
												"dependentVariableId": "olm-bundle/{{ bundle.BundleID }}",
												"orderPreference":     `semverCompare(v1.Properties["olm.package.version"], v2.Properties["olm.package.version"]) > 0`,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	packageUniquenessConstraintsSource := resolver.Task{
		TaskType: resolver.TaskTypeEdits,
		EditTask: &resolver.EditTask{
			Edits: []resolver.Edit{
				{
					Type:       resolver.EditTypeVariable,
					Action:     resolver.ActionUpsert,
					Kind:       "olm.variable.uniqueness",
					VariableID: "package-uniqueness",
				}, {
					Type:           resolver.EditTypeConstraint,
					Action:         resolver.ActionAdd,
					Kind:           "olm.variable.uniqueness",
					VariableID:     "package-uniqueness",
					ConstraintType: "AtMost",
					ConstraintID:   `package-uniqueness/{{ curVar.Property("olm.package.name") }}`,
					Params: map[string]interface{}{
						"n":               1,
						"variableId":      `{{ curVar.VariableID }}`,
						"orderPreference": `semverCompare(v1.Properties["olm.package.version"], v2.Properties["olm.package.version"]) > 0`,
					},
				},
			},
		},
	}

	gvkUniquenessConstraintsSource := resolver.Task{
		TaskType: resolver.TaskTypeTemplate,
		TemplateTask: &resolver.TemplateTask{
			Template: resolver.Template{
				ForLoop: resolver.ForLoop{
					Variable: "gvk",
					Query:    `{{ curVar.Property("olm.gvk.provided") }}`,
					Do: resolver.Do{
						DoType: resolver.DoTypeEdits,
						EditTask: &resolver.EditTask{
							Edits: []resolver.Edit{
								{
									Type:       resolver.EditTypeVariable,
									Action:     resolver.ActionUpsert,
									Kind:       "olm.variable.uniqueness",
									VariableID: "gvk-uniqueness",
								}, {
									Type:           resolver.EditTypeConstraint,
									Action:         resolver.ActionAdd,
									Kind:           "olm.variable.uniqueness",
									VariableID:     "gvk-uniqueness",
									ConstraintType: "AtMost",
									ConstraintID:   `gvk-uniqueness/{{ gvk.Group }}:{{ gvk.Version }}:{{ gvk.Kind }}`,
									Params: map[string]interface{}{
										"n":               1,
										"variableId":      `{{ curVar.VariableID }}`,
										"orderPreference": `semverCompare(v1.Properties["olm.package.version"], v2.Properties["olm.package.version"]) > 0`,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	variableSources := []resolver.VariableSource{
		resolver.NewDeclarativeVariableSource("required-package/kuadrant-operator", "", requiredPackageSource, DB),
		resolver.NewDeclarativeVariableSource("required-package/:add-mandatory", "olm.variable.required-package", requiredPackageMandatorySource, DB),
		resolver.NewDeclarativeVariableSource("required-package/:bundles", "olm.variable.required-package", requiredPackageBundlesSource, DB),
		resolver.NewDeclarativeVariableSource("required-package/:bundle-dependencies", "olm.variable.bundle", requiredPackageDependencySource, DB),
		resolver.NewDeclarativeVariableSource("bundle/:bundle-dependencies", "olm.variable.bundle", bundlePackageDependenciesSource, DB),
		resolver.NewDeclarativeVariableSource("bundle/:gvk-dependencies", "olm.variable.bundle", bundleGVKDependenciesSource, DB),
		resolver.NewDeclarativeVariableSource("uniqueness/:package", "olm.variable.bundle", packageUniquenessConstraintsSource, DB),
		resolver.NewDeclarativeVariableSource("uniqueness/:gvk", "olm.variable.bundle", gvkUniquenessConstraintsSource, DB),

		// upgrade edges
		resolver.NewDeclarativeVariableSource("bundle/:installed", "", installedPackageSource, DB),
		resolver.NewDeclarativeVariableSource("bundle/:upgrades", "olm.variable.installed-package", installedPackageUpgradeSource, DB),
	}

	for _, v := range variableSources {
		j, _ := yaml.Marshal(v)
		fmt.Println(string(j))
	}

	//resolution := resolver.NewResolution(variableSources...)
	//
	//s, _ := solver.NewDeppySolver(nil, resolution)
	//solution, err := s.Solve(context.Background(), solver.AddAllVariablesToSolution())
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//for _, v := range solution.SelectedVariables() {
	//	j, _ := json.MarshalIndent(v, "", "  ")
	//	fmt.Println(string(j))
	//}
}
