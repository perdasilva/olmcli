package stages

import (
	"context"
	"errors"
	"fmt"
	"github.com/blang/semver/v4"
	"github.com/google/uuid"
	"github.com/perdasilva/olmcli/pkg/deppy"
	"github.com/perdasilva/olmcli/pkg/deppy/resolver"
	"github.com/perdasilva/olmcli/pkg/deppy/variables"
	"github.com/perdasilva/olmcli/pkg/manager"
	"github.com/perdasilva/olmcli/pkg/store"
	"slices"
	"strings"
)

const (
	RequiredPackageVariableKind    = "olm.variable.required-package"
	RequiredPackageNameProperty    = "olm.package.name"
	RequiredPackageVersionProperty = "olm.package.version"

	BundleVariableKind           = "olm.variable.bundle"
	PackageNameBundleProperty    = "olm.bundle.package-name"
	PackageVersionBundleProperty = "olm.bundle.package-version"
	RepositoryBundleProperty     = "olm.bundle.registry"
	BundleProperty               = "olm.bundle"

	UniquenessConstraintVariableKind = "olm.variable.uniqueness-constraint"

	DependencyConstraintVariableKind = "olm.variable.dependency-constraint"
)

// Task is a blocking function that takes a context, a stdin channel, a stdout channel, and a stderr channel.
// A task is executed by a Stage, stdin, and stdout are managed by the stage and passed to the task.
type Task func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error

// Stage represents a pipeline stage that executes a task function and manages its input and output channels.
// A stage is responsible for creating its output and error channels and well as close them.
type Stage struct {
	task Task
}

// Run executes the stage's task and manages the output and error channels for the task. The Task is executed in a go function.
// The execution is finished once the task returns or the context is cancelled.
func (s Stage) Run(ctx context.Context, stdin <-chan deppy.Variable) (chan deppy.Variable, chan error) {
	stdout := make(chan deppy.Variable)
	stderr := make(chan error)
	go func() {
		defer close(stdout)
		defer close(stderr)
		if err := s.task(ctx, stdin, stdout); err != nil {
			stderr <- err
		}
	}()
	return stdout, stderr
}

// Pipe is a utility function that copies all variables from stdin to stdout
// it is useful when a stage is done with its work and just wants to pass all variables to the next stage
func Pipe(stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) {
	for v := range stdin {
		stdout <- v
	}
}

// RequiredPackages is a pipeline stage that produces required-package variables given a list of package names
func RequiredPackages(packageNames ...string) Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			// add our required package down the pipe
			for _, packageName := range packageNames {
				varId := deppy.Identifier(fmt.Sprintf("required-package/%s", packageName))
				v := variables.NewMutableVariable(varId, RequiredPackageVariableKind, map[string]interface{}{
					RequiredPackageNameProperty:    packageName,
					RequiredPackageVersionProperty: ">=0.0.0",
				})
				_ = v.AddMandatory("mandatory")
				stdout <- v
			}

			// pass any variables you see to the next stage
			Pipe(stdin, stdout)
			return nil
		},
	}
}

// RequiredPackageBundles is a stage that produces bundle variables to satisfy required-package variables
// It also creates another variable which activates if the required-package variable is activated, it also
// has dependencies on any bundle variables that satisfy the required-package variable request.
// If the required-package variable is mandatory, this will have the effect of ensuring that at least one bundle
// that can satisfy it is selected (or fail trying)
func RequiredPackageBundles(registryManager manager.Manager) Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						return nil
					}

					// if this is a required-package variable, add a bundle variable
					if variable.Kind() == RequiredPackageVariableKind {
						// extract package name and version range parameters
						packageName, err := getPropertyAsString(variable, RequiredPackageNameProperty)
						if err != nil {
							return err
						}
						packageVersion, err := getPropertyAsString(variable, RequiredPackageVersionProperty)
						if err != nil {
							return err
						}
						semverRange, err := semver.ParseRange(packageVersion)
						if err != nil {
							return err
						}

						// query registry
						bundles, err := registryManager.GetBundlesForPackage(ctx, packageName, store.InVersionRange(semverRange))
						if err != nil {
							return err
						}

						dependencyConstraintVariable := variables.NewMutableVariable(variable.Identifier()+"/bundle-dependency", DependencyConstraintVariableKind, map[string]interface{}{})

						_ = dependencyConstraintVariable.AddReverseDependency("activating-variable", variable.Identifier())
						_ = dependencyConstraintVariable.AddDependency("bundle.dependencies")

						// add a bundle variable for each bundle that matches the required package
						sortBundlesByVersionDesc(bundles)
						for _, bundle := range bundles {
							bundle := bundle
							v := variables.NewMutableVariable(deppy.Identifier(bundle.BundleID), BundleVariableKind, map[string]interface{}{
								PackageNameBundleProperty:    bundle.GetPackageName(),
								PackageVersionBundleProperty: bundle.GetVersion(),
								RepositoryBundleProperty:     bundle.Repository,
								BundleProperty:               bundle,
							})
							_ = dependencyConstraintVariable.AddDependency("bundle.dependencies", v.Identifier())
							stdout <- v
						}
						stdout <- dependencyConstraintVariable
					}

					// pass any variables you see to the next stage
					stdout <- variable
				}
			}
		},
	}
}

// BundleDependencies is a stage that produces dependency-constraint variables for each bundle variable
// to reflect each bundle's package and gvk dependencies. This dependency constraint variable is activated
// whenever the parent bundle variable is activated. The dependency-constraint variable also has a dependency constraint
// on the bundle variables that satisfy each of the parent bundle's package and gvk dependencies. This ensures that
// bundle dependencies are pulled into the solution when the parent bundle is selected (or fail trying).
// This stage also de-duplicates bundle variables by ensuring that only one bundle variable exists for each bundle
func BundleDependencies(registryManager manager.Manager) Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			processed := map[deppy.Identifier]struct{}{}

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						return nil
					}
					_, ok := processed[variable.Identifier()]
					if variable.Kind() == BundleVariableKind && !ok {
						// get bundle from bundle variable
						bundleValue, ok := variable.GetProperty(BundleProperty)
						if !ok {
							return errors.New("bundle not found in variable")
						}
						variableBundle, ok := bundleValue.(store.CachedBundle)
						if !ok {
							return errors.New("bundle not found in variable")
						}

						// create queue for breadth-first search
						bundleQueue := []store.CachedBundle{variableBundle}

						// breadth-first search for all dependencies
						for len(bundleQueue) > 0 {
							var head store.CachedBundle
							head, bundleQueue = bundleQueue[0], bundleQueue[1:]

							// skip processed bundles
							if _, ok := processed[deppy.Identifier(head.BundleID)]; ok {
								continue
							}
							// mark head as processed
							processed[deppy.Identifier(head.BundleID)] = struct{}{}

							v := variables.NewMutableVariable(deppy.Identifier(head.BundleID)+"/package-dependencies", DependencyConstraintVariableKind, map[string]interface{}{})
							_ = v.AddReverseDependency("activating-variable", deppy.Identifier(head.BundleID))

							for _, packageDependency := range head.PackageDependencies {
								versionRange, err := semver.ParseRange(packageDependency.Version)
								if err != nil {
									return err
								}

								// query registry
								bundles, err := registryManager.GetBundlesForPackage(ctx, packageDependency.PackageName, store.InVersionRange(versionRange))
								if err != nil {
									return err
								}

								sortBundlesByVersionDesc(bundles)
								for i, _ := range bundles {
									_ = v.AddDependency(deppy.Identifier(packageDependency.PackageName+"/"+packageDependency.Version), deppy.Identifier(bundles[i].BundleID))
									bundleQueue = append(bundleQueue, bundles[i])
								}
							}

							if len(head.PackageDependencies) > 0 {
								stdout <- v
							}

							v = variables.NewMutableVariable(deppy.Identifier(head.BundleID)+"/gvk-dependencies", DependencyConstraintVariableKind, map[string]interface{}{})
							_ = v.AddReverseDependency("activating-variable", deppy.Identifier(head.BundleID))

							for _, gvkDependency := range head.RequiredApis {
								bundles, err := registryManager.GetBundlesForGVK(ctx, gvkDependency.Group, gvkDependency.Version, gvkDependency.Kind)
								if err != nil {
									return err
								}

								sortBundlesByVersionDesc(bundles)
								for i, _ := range bundles {
									_ = v.AddDependency(deppy.Identifier(gvkDependency.Group+"/"+gvkDependency.Version+"/"+gvkDependency.Kind), deppy.Identifier(bundles[i].BundleID))
									bundleQueue = append(bundleQueue, bundles[i])
								}
							}

							if len(head.RequiredApis) > 0 {
								stdout <- v
							}

							if head.BundleID == variableBundle.BundleID {
								stdout <- variable
							} else {
								stdout <- variables.NewMutableVariable(deppy.Identifier(head.BundleID), BundleVariableKind, map[string]interface{}{
									PackageNameBundleProperty:    head.GetPackageName(),
									PackageVersionBundleProperty: head.GetVersion(),
									RepositoryBundleProperty:     head.Repository,
									BundleProperty:               head,
								})
							}
						}
					} else if !ok {
						stdout <- variable
					}
				}
			}
		},
	}
}

// UniquenessConstraints is a stage that produces a uniqueness-constraints variables with constraints against
// individual packages and gvks that ensure that at most 1 bundle for each package and gvk is selected in the solution.
// This is mainly to prevent CRDs stepping on each other's toes - this may need to be loosened up to perhaps just gvk?
func UniquenessConstraints() Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			v := variables.NewMutableVariable("uniqueness-constraints", UniquenessConstraintVariableKind, map[string]interface{}{})
			_ = v.AddMandatory("mandatory")

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						stdout <- v
						return nil
					}
					if variable.Kind() == BundleVariableKind {
						bundleValue, ok := variable.GetProperty(BundleProperty)
						if !ok {
							return errors.New("bundle not found in variable")
						}
						bundle, ok := bundleValue.(store.CachedBundle)
						if !ok {
							return errors.New("bundle not found in variable")
						}

						// add bundle identifier to its package uniqueness constraint
						_ = v.AddAtMost(deppy.Identifier("package-uniqueness/"+bundle.GetPackageName()), 1, variable.Identifier())

						for _, gvk := range bundle.ProvidedApis {
							// add bundle identifier to its gvk uniqueness constraint
							_ = v.AddAtMost(deppy.Identifier("gvk-uniqueness/"+gvk.Group+"/"+gvk.Version+"/"+gvk.Kind), 1, variable.Identifier())
						}
					}
					stdout <- variable
				}
			}
		},
	}
}

// SATSolve is a stage that runs the deppy solver against all the variables coming from preceding stages. It acts as
// a filter, only lettering variables through that are selected by the solver.
func SATSolve() Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			var problem []deppy.Variable
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						// once you've collected all variables, you have your problem
						deppyResolver := resolver.NewDeppyResolver()
						solution, err := deppyResolver.Solve(ctx, NewResolutionProblem(deppy.Identifier(uuid.NewString()), problem))
						if err != nil {
							return err
						}
						if len(solution.NotSatisfiable()) > 0 {
							return err
						}
						for i, _ := range solution.SelectedVariables() {
							stdout <- solution.SelectedVariables()[i]
						}
						return nil
					}
					problem = append(problem, variable)
				}
			}
		},
	}
}

type FilterFn func(v deppy.Variable) bool

// Filter is a stage that filters variables based on a provided filter function
func Filter(fn FilterFn) Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						return nil
					}
					if fn(variable) {
						stdout <- variable
					}
				}
			}
		},
	}
}

// getPropertyAsString is a helper function that extracts a string property from a variable
func getPropertyAsString(v deppy.Variable, key string) (string, error) {
	value, ok := v.GetProperty(key)
	if !ok {
		return "", fmt.Errorf("property %s not found", key)
	}
	valueAsString, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("property %s is not a string", key)
	}
	return valueAsString, nil
}

// sortBundlesByVersionDesc is a helper function that sorts bundles by version in descending order
func sortBundlesByVersionDesc(bundles []store.CachedBundle) {
	slices.SortStableFunc(bundles, func(i, j store.CachedBundle) int {
		v1 := semver.MustParse(i.GetVersion())
		v2 := semver.MustParse(j.GetVersion())

		if v1.EQ(v2) {
			return strings.Compare(i.GetPackageName(), j.GetPackageName())
		}
		return -1 * v1.Compare(v2)
	})
}
