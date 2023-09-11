package main

import (
	"context"
	"fmt"
	"github.com/perdasilva/olmcli/pkg/deppy"
	"github.com/perdasilva/olmcli/pkg/deppy/pipeline"
	"github.com/perdasilva/olmcli/pkg/deppy/pipeline/stages"
	"github.com/perdasilva/olmcli/pkg/deppy/resolver"
	"github.com/perdasilva/olmcli/pkg/manager"
	"github.com/sirupsen/logrus"
	"log/slog"
	"os"
	"time"
)

func main() {
	// create a context for the pipeline execution
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a registry manager to access catalog data
	registryManager, err := manager.NewManager("", logrus.StandardLogger())
	if err != nil {
		panic(err)
	}

	// get a list of package names from the command line
	packageNames := os.Args[1:]
	//if len(packageNames) == 0 {
	//	log.Fatal("no package names provided")
	//}

	const ssteEnabled = false

	// create problem pipeline
	// variables flow from one stage to the next
	// each stage adds new variables to the problem or filters out variables from the problem
	// variables are immutable - once they are created, they cannot be changed
	// constraints can be applied to an existing variable by creating a new variable
	// with a reverse dependency on the original variable and any additional constraints.
	// The reverse dependency variable will be activated by the solver iff the original variable is activated
	ppl := pipeline.NewPipeline(
		// define the initial state/constraints
		stages.RequiredPackages(packageNames...),
		stages.SSTE(ssteEnabled),
		stages.InstalledBundles("prometheus", "0.37.0"),
		stages.SSTE(ssteEnabled),
		// emit bundle-variables for each bundle that provides a required-package
		// and a constraint variable that ensures that at least bundle is selected
		// when the required-package is activated (selected by the solver)
		stages.InitialRequiredBundleSet(registryManager),
		stages.SSTE(ssteEnabled),
		// enable upgrade edges
		stages.ChannelsAndUpgradeEdges(registryManager.GetPackageDatabase()),
		stages.SSTE(ssteEnabled),

		// emit bundle-variables for each bundle that provides a package or gvk dependency
		// emit bundle-dependency-variable that activates when the parent bundle-variable is activated
		// the bundle-dependency-variable also has dependency constraints against the bundles that fulfil the
		// dependencies
		stages.BundleDependencies(registryManager),
		stages.SSTE(ssteEnabled),

		// look at every bundle-variable and emit a uniqueness-constraint-variable
		// that has constraints that ensures that at most 1 bundle-variable is selected / package and / gvk
		stages.UniquenessConstraints(),
		stages.SSTE(ssteEnabled),
	)

	// execute the pipeline to collect the problem
	start := time.Now()
	resolutionProblem, err := ppl.Run(ctx)
	if err != nil {
		slog.Error("error executing pipeline", "runtime", time.Since(start), "error", err)
		return
	}
	variables, _ := resolutionProblem.GetVariables()
	for _, v := range variables {
		slog.Info(fmt.Sprintf("%s", v))
	}
	slog.Info("problem", "problem_id", resolutionProblem.ResolutionProblemID(), "runtime", time.Since(start), "variables", extractVariableIDs(variables))

	// solve the problem
	start = time.Now()
	r := resolver.NewDeppyResolver()
	solution, err := r.Solve(ctx, resolutionProblem)
	if err != nil {
		slog.Error("error executing problem", "problem_id", resolutionProblem.ResolutionProblemID(), "runtime", time.Since(start), "error", err)
		return
	}
	if len(solution.NotSatisfiable()) > 0 {
		slog.Error("problem not satisfiable", "problem_id", resolutionProblem.ResolutionProblemID(), "runtime", time.Since(start), "error", solution.NotSatisfiable())
		return
	}
	slog.Info("solution", "problem_id", resolutionProblem.ResolutionProblemID(), "runtime", time.Since(start), "solution", extractVariableIDs(solution.SelectedVariables()))
}

func extractVariableIDs(variables interface{}) []deppy.Identifier {
	switch vars := variables.(type) {
	case []deppy.Variable:
		ids := make([]deppy.Identifier, len(vars))
		for i, variable := range vars {
			ids[i] = variable.Identifier()
		}
		return ids
	case map[deppy.Identifier]deppy.Variable:
		ids := make([]deppy.Identifier, len(vars))
		for id, _ := range vars {
			ids = append(ids, id)
		}
		return ids
	default:
		panic("unknown variable type")
	}
}
