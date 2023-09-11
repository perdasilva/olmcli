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

	BundleDependencyConstraintVariableActivatingVariableIDProperty = "olm.variable.activating-variable"

	BundleVariableKind           = "olm.variable.bundle"
	PackageNameBundleProperty    = "olm.bundle.package-name"
	PackageVersionBundleProperty = "olm.bundle.package-version"
	RepositoryBundleProperty     = "olm.bundle.registry"
	BundleProperty               = "olm.bundle"

	UniquenessConstraintVariableKind = "olm.variable.uniqueness-constraint"

	BundleDependencyConstraintVariableKind = "olm.variable.bundle-dependency-constraint"
	ChannelVariableKind                    = "olm.variable.channel"
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

func InstalledBundles(installedBundles ...string) Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			for i := 0; i < len(installedBundles); i += 2 {
				packageName := installedBundles[i]
				version := installedBundles[i+1]
				varId := deppy.Identifier(fmt.Sprintf("installed-package/%s", packageName))
				v := variables.NewMutableVariable(varId, RequiredPackageVariableKind, map[string]interface{}{
					RequiredPackageNameProperty:    packageName,
					RequiredPackageVersionProperty: version,
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

// InitialRequiredBundleSet is a stage that produces bundle variables to satisfy required-package variables
// It also creates another variable which activates if the required-package variable is activated, it also
// has dependencies on any bundle variables that satisfy the required-package variable request.
// If the required-package variable is mandatory, this will have the effect of ensuring that at least one bundle
// that can satisfy it is selected (or fail trying)
func InitialRequiredBundleSet(registryManager manager.Manager) Stage {
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

					// if this is a required-package variable, add a bundle variable
					if variable.Kind() == RequiredPackageVariableKind {
						// extract package name and version range parameters
						packageName, err := getProperty[string](variable, RequiredPackageNameProperty)
						if err != nil {
							return err
						}
						semverRange, err := getProperty[string](variable, RequiredPackageVersionProperty)
						if err != nil {
							return err
						}

						// query registry
						bundles, err := registryManager.GetBundlesForPackage(ctx, packageName, store.InVersionRange(semver.MustParseRange(semverRange)))
						if err != nil {
							return err
						}

						dependencyConstraintVariable := variables.NewMutableVariable(variable.Identifier()+"/bundle-dependency", BundleDependencyConstraintVariableKind, map[string]interface{}{
							BundleDependencyConstraintVariableActivatingVariableIDProperty: variable,
						})

						_ = dependencyConstraintVariable.AddReverseDependency("activating-variable", variable.Identifier())
						_ = dependencyConstraintVariable.AddDependency("bundle.dependencies")

						// add a bundle variable for each bundle that matches the required package
						sortBundlesByVersionDesc(bundles)
						var depIds []deppy.Identifier
						for _, bundle := range bundles {
							v := newBundleVariable(bundle)
							depIds = append(depIds, v.Identifier())
							if _, ok := processed[v.Identifier()]; !ok {
								stdout <- v
								processed[v.Identifier()] = struct{}{}
							}
						}
						_ = dependencyConstraintVariable.AddDependency("bundle.dependencies", depIds...)
						stdout <- dependencyConstraintVariable
					}

					// pass any variables you see to the next stage
					stdout <- variable
				}
			}
		},
	}
}

// ChannelsAndUpgradeEdges is a stage that slurps required-package variables with ids starting with "installed-package"
// i.e. the variables that represent installed cluster content
// and replaces them with a copy of the variable (but not its constraints) and models instead channel and upgrade constraints
// this stage enables upgrades
func ChannelsAndUpgradeEdges(packageDB store.PackageDatabase) Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			processed := map[string]struct{}{}
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						return nil
					}

					// if this is a required-package variable, add a bundle variable
					if variable.Kind() == BundleDependencyConstraintVariableKind && strings.HasPrefix(variable.Identifier().String(), "installed-package/") {
						reqPackageVar, err := getProperty[deppy.Variable](variable, BundleDependencyConstraintVariableActivatingVariableIDProperty)
						if err != nil {
							return err
						}
						if reqPackageVar.Kind() != RequiredPackageVariableKind {
							return fmt.Errorf("expected required-package variable, got %s", reqPackageVar.Kind())
						}
						// extract package name and version range parameters
						packageName, err := getProperty[string](reqPackageVar, RequiredPackageNameProperty)
						if err != nil {
							return err
						}

						if _, ok := processed[packageName]; ok {
							stdout <- variable
							continue
						}
						processed[packageName] = struct{}{}

						var pkgs []store.CachedPackage
						err = packageDB.IteratePackages(ctx, func(pkg *store.CachedPackage) error {
							if pkg.GetName() == packageName {
								pkgs = append(pkgs, *pkg)
							}
							return nil
						})
						if err != nil {
							return err
						}

						// todo: we assume only one package - we'll need to expand later to package name collisions across repositories
						if len(pkgs) == 0 {
							return fmt.Errorf("package not found '%s'", packageName)
						}
						if len(pkgs) > 1 {
							return fmt.Errorf("too many packages found '%s'", pkgs)
						}

						version, err := getProperty[string](reqPackageVar, RequiredPackageVersionProperty)
						if err != nil {
							return err
						}

						sourceBundles, err := packageDB.GetBundlesForPackage(ctx, packageName, store.InVersionRange(semver.MustParseRange("=="+version)))
						if err != nil {
							return err
						}

						if len(sourceBundles) == 0 {
							return errors.New("no bundles found for package")
						}
						if len(sourceBundles) > 1 {
							return errors.New("multiple bundles found for package")
						}

						pkg := pkgs[0]
						bundleDependencyVariable := variables.NewMutableVariable(variable.Identifier(), variable.Kind(), variable.GetProperties())
						_ = bundleDependencyVariable.AddReverseDependency("activating-variable", reqPackageVar.Identifier())
						var depIDs []deppy.Identifier
						for _, channel := range pkg.Channels {
							chVar := variables.NewMutableVariable(ChannelVariableID(pkg.Name, channel.Name), ChannelVariableKind, map[string]interface{}{})
							depIDs = append(depIDs, chVar.Identifier())
							if channel.Name == pkg.DefaultChannelName {
								depIDs[0], depIDs[len(depIDs)-1] = depIDs[len(depIDs)-1], depIDs[0]
							}
							bundles, err := packageDB.GetBundlesForPackage(ctx, pkg.Name, store.InChannel(channel.Name), store.InVersionRange(semver.MustParseRange(">"+version)), store.Replaces(sourceBundles[0].CsvName))
							if err != nil {
								return err
							}

							sortBundlesByVersionDesc(bundles)
							// "upgrading" to yourself is also an option (i.e. keep things as they are)
							// make it the default one by putting it at the head of the dependency list
							bundles = append(sourceBundles, bundles...)
							for _, bundle := range bundles {
								v := newBundleVariable(bundle)
								_ = chVar.AddDependency("bundles", v.Identifier())
								stdout <- v
							}
							stdout <- chVar
						}
						_ = bundleDependencyVariable.AddDependency("channels", depIDs...)
						stdout <- bundleDependencyVariable
					} else {
						// pass any variables you see to the next stage
						stdout <- variable
					}
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

							v := variables.NewMutableVariable(deppy.Identifier(head.BundleID)+"/package-dependencies", BundleDependencyConstraintVariableKind, map[string]interface{}{})
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

							v = variables.NewMutableVariable(deppy.Identifier(head.BundleID)+"/gvk-dependencies", BundleDependencyConstraintVariableKind, map[string]interface{}{})
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
								stdout <- newBundleVariable(head)
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

func ChannelVariableID(packageName, channelName string) deppy.Identifier {
	return deppy.Identifier(fmt.Sprintf("channel/%s/%s", packageName, channelName))
}

// UniquenessConstraints is a stage that produces a uniqueness-constraints variables with constraints against
// individual packages and gvks that ensure that at most 1 bundle for each package and gvk is selected in the solution.
// This is mainly to prevent CRDs stepping on each other's toes - this may need to be loosened up to perhaps just gvk?
func UniquenessConstraints() Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			v := variables.NewMutableVariable("uniqueness-constraints", UniquenessConstraintVariableKind, map[string]interface{}{})
			_ = v.AddMandatory("mandatory")

			pkgMap := map[string][]store.CachedBundle{}
			gvkMap := map[string][]store.CachedBundle{}

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						for packageName, deps := range pkgMap {
							sortBundlesByVersionDesc(deps)
							depIDs := make([]deppy.Identifier, len(deps))
							for i, dep := range deps {
								depIDs[i] = deppy.Identifier(dep.BundleID)
							}
							_ = v.AddAtMost(deppy.Identifier("package-uniqueness/"+packageName), 1, depIDs...)
						}

						for gvk, deps := range gvkMap {
							sortBundlesByVersionDesc(deps)
							depIDs := make([]deppy.Identifier, len(deps))
							for i, dep := range deps {
								depIDs[i] = deppy.Identifier(dep.BundleID)
							}
							_ = v.AddAtMost(deppy.Identifier("gvk-uniqueness/"+gvk), 1, depIDs...)
						}
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
						pkgMap[bundle.PackageName] = append(pkgMap[bundle.PackageName], bundle)

						for _, gvk := range bundle.ProvidedApis {
							gvkMap[gvk.Group+"/"+gvk.Version+"/"+gvk.Kind] = append(gvkMap[gvk.Group+"/"+gvk.Version+"/"+gvk.Kind], bundle)
						}
					}
					stdout <- variable
				}
			}
		},
	}
}

func SSTE(enabled bool) Stage {
	return Stage{
		task: func(ctx context.Context, stdin <-chan deppy.Variable, stdout chan<- deppy.Variable) error {
			var vars []deppy.Variable
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case variable, hasNext := <-stdin:
					if !hasNext {
						for _, v := range vars {
							stdout <- v
						}
						return nil
					}
					if !enabled {
						stdout <- variable
					} else {
						vars = append(vars, variable)
					}
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

func getProperty[T any](v deppy.Variable, key string) (T, error) {
	value, ok := v.GetProperty(key)
	if !ok {
		return *new(T), fmt.Errorf("property %s not found", key)
	}
	valueAsT, ok := value.(T)
	if !ok {
		return *new(T), fmt.Errorf("property %s is not a %T", key, valueAsT)
	}
	return valueAsT, nil
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
		return -1 * v1.Compare(v2)
	})
}

// sortBundlesByVersionAsc is a helper function that sorts bundles by version in ascending order
func sortBundlesByVersionAsc(bundles []store.CachedBundle) {
	slices.SortStableFunc(bundles, func(i, j store.CachedBundle) int {
		v1 := semver.MustParse(i.GetVersion())
		v2 := semver.MustParse(j.GetVersion())
		return v1.Compare(v2)
	})
}

func newBundleVariable(bundle store.CachedBundle) deppy.Variable {
	return variables.NewMutableVariable(deppy.Identifier(bundle.BundleID), BundleVariableKind, map[string]interface{}{
		PackageNameBundleProperty:    bundle.GetPackageName(),
		PackageVersionBundleProperty: bundle.GetVersion(),
		RepositoryBundleProperty:     bundle.Repository,
		BundleProperty:               bundle,
	})
}
