package main

import (
	"context"
	"log"
	"os"
	"path"
	"strconv"

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

	variableSources, err := resolver.FromDir("hack/declarative_source/fromdir/sudoku", DB)
	if err != nil {
		log.Fatal(err)
	}

	resolution := resolver.NewResolution(variableSources...)

	s := solver.NewDeppySolver(nil, resolution)
	solution, err := s.Solve(context.Background(), solver.AddAllVariablesToSolution(), solver.DisableOrderPreference())
	if err != nil {
		log.Fatal(err)
	}

	board := make([][]int, 9)
	var vars []resolver.Variable
	for _, v := range solution.SelectedVariables() {
		t := v.(*resolver.Variable)
		if t.Kind() == "cell" {
			row, _ := strconv.Atoi(t.Property("row").(string))
			col, _ := strconv.Atoi(t.Property("col").(string))
			num, _ := strconv.Atoi(t.Property("num").(string))
			if board[row] == nil {
				board[row] = make([]int, 9)
			}
			board[row][col] = num + 1
		} else {
			vars = append(vars, *t)
		}
		//j, _ := json.MarshalIndent(v, "", "  ")
		//fmt.Println(string(j))
	}

	//for _, v := range vars {
	//	println(v.VariableID + ": ")
	//	dep := v.VarConstraints["num-pick-one"].(*resolver.DependencyConstraint)
	//	var hasOne bool = false
	//	for _, c := range dep.Order() {
	//		isSelected := solution.IsSelected(c)
	//		hasOne = hasOne || isSelected
	//		fmt.Printf("%s: %t ", c, isSelected)
	//	}
	//	if !hasOne {
	//		fmt.Printf("No solution found for %s", v.VariableID)
	//	}
	//	println()
	//}

	for _, row := range board {
		for _, num := range row {
			print(num, " ")
		}
		println()
	}

	println()
}
