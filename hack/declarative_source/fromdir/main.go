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

	variableSources, err := resolver.FromDir("hack/declarative_source/fromdir", DB)
	if err != nil {
		log.Fatal(err)
	}

	resolution := resolver.NewResolution(variableSources...)

	s, _ := solver.NewDeppySolver(nil, resolution)
	solution, err := s.Solve(context.Background(), solver.AddAllVariablesToSolution())
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range solution.SelectedVariables() {
		j, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(j))
	}
}
