package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/operator-framework/deppy/pkg/deppy"
	"github.com/operator-framework/deppy/pkg/deppy/input"
	"github.com/operator-framework/deppy/pkg/deppy/solver"

	"github.com/perdasilva/olmcli/internal/resolver"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/sirupsen/logrus"
)

type dummyVariableSource struct {
	variables []deppy.Variable
}

func (d dummyVariableSource) GetVariables(ctx context.Context, entitySource input.EntitySource) ([]deppy.Variable, error) {
	return d.variables, nil
}

var _ input.VariableSource = &dummyVariableSource{}

func main() {
	logger := logrus.Logger{
		Out:   os.Stderr,
		Level: logrus.InfoLevel,
		Formatter: &logrus.TextFormatter{
			ForceColors:     true,
			DisableColors:   false,
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		},
	}

	packageDatabase, err := store.NewPackageDatabase(path.Join("/home/perdasilva/.olm", "olm.db"), &logger)
	if err != nil {
		panic(err)
	}

	variableSources := []resolver.VariableSource{
		resolver.NewRequiredPackageDependencySource(),
		resolver.NewRequiredPackageBundlesSource(packageDatabase),
		resolver.NewBundleGVKDependenciesSource(packageDatabase),
		resolver.NewPackageUniquenessConstraintsSource(),
		resolver.NewGVKUniquenessConstraintsSource(),
	}

	resolution := resolver.NewResolution(variableSources...)
	ctx := context.Background()

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
