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

	"github.com/perdasilva/olmcli/internal/repo"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// addRepoCmd represents the add command
var addRepoCmd = &cobra.Command{
	Use:  "repo",
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := repo.NewManager(viper.GetString("configPath"), &logger)
		if err != nil {
			return err
		}

		if err := manager.AddRepository(context.Background(), args[0]); err != nil {
			return err
		}
		defer manager.Close(context.Background())
		return nil
	},
}

func init() {
	addCmd.AddCommand(addRepoCmd)
}
