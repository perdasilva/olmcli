/*
Copyright © 2022 Per G. da Silva <pegoncal@redhat.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/perdasilva/olmcli/internal/repo"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// searchPackageCmd represents the search command
var searchPackageCmd = &cobra.Command{
	Use:  "pkg",
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := repo.NewManager(viper.GetString("configPath"), &logger)
		if err != nil {
			return err
		}
		defer manager.Close(context.Background())

		pkgs, err := manager.SearchPackages(context.Background(), args[0])
		if err != nil {
			return err
		}

		if len(pkgs) == 0 {
			fmt.Println("No packages found...")
			return nil
		}

		// initialize tabwriter
		w := new(tabwriter.Writer)

		// minwidth, tabwidth, padding, padchar, flags
		w.Init(os.Stdout, 8, 8, 0, '\t', 0)
		defer w.Flush()

		fmt.Fprintf(w, "%s\t%s\t%s\t\n", "PACKAGE", "DEFAULT CHANNEL", "REPOSITORY")
		for _, pkg := range pkgs {
			fmt.Fprintf(w, "%s\t%s\t%s\t\n", pkg.Name, pkg.DefaultChannelName, pkg.Repository)
		}
		return nil
	},
}

func init() {
	searchCmd.AddCommand(searchPackageCmd)
}
