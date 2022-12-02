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

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// listRepoCmd represents the list command
var listRepoCmd = &cobra.Command{
	Use: "repo",
	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := manager.NewManager(viper.GetString("configPath"), &logger)
		if err != nil {
			return err
		}
		defer manager.Close()

		repos, err := manager.ListRepositories(context.Background())
		if err != nil {
			return err
		}

		if len(repos) == 0 {
			fmt.Println("No repositories found...")
			return nil
		}

		// initialize tabwriter
		w := new(tabwriter.Writer)

		// minwidth, tabwidth, padding, padchar, flags
		w.Init(os.Stdout, 8, 8, 0, '\t', 0)
		defer w.Flush()

		fmt.Fprintf(w, "%s\t%s\t\n", "REPOSITORY", "SOURCE")
		for _, repo := range repos {
			fmt.Fprintf(w, "%s\t%s\t\n", repo.RepositoryName, repo.RepositorySource)
		}
		return nil
	},
}

func init() {
	listCmd.AddCommand(listRepoCmd)
}
