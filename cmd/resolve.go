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
	"strings"

	"github.com/perdasilva/olmcli/internal/manager"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/jedib0t/go-pretty/v6/list"
)

// resolveCmd represents the resolve command
var resolveCmd = &cobra.Command{
	Use:   "resolve",
	Short: "run resolution on a package",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := manager.NewManager(viper.GetString("configPath"), &logger)
		if err != nil {
			return err
		}
		installables, err := manager.Resolve(context.Background(), args[0])
		if err != nil {
			return err
		}
		l := list.NewWriter()
		l.SetStyle(list.StyleConnectedRounded)
		l.AppendItem("Resolved Bundles")
		l.Indent()
		for _, installable := range installables {
			l.AppendItem(installable.BundleID)
			l.Indent()
			for dep, _ := range installable.Dependencies {
				l.AppendItem(dep)
			}
			l.UnIndent()
		}
		l.UnIndent()
		for _, line := range strings.Split(l.Render(), "\n") {
			logger.Printf(line)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(resolveCmd)
}
