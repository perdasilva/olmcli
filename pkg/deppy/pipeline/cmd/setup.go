package cmd

import (
	"context"
	"github.com/google/uuid"
	"github.com/perdasilva/olmcli/pkg/manager"
	"github.com/perdasilva/olmcli/pkg/store"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

const configPath = "."
const operatorhubio = "quay.io/operatorhubio/catalog:latest"

// setupCmd sets up the package database
var setupCmd = &cobra.Command{
	Use: "setup",
	RunE: func(cmd *cobra.Command, args []string) error {
		lgr := logrus.New()
		manager, err := manager.NewManager(configPath, lgr)
		if err != nil {
			return err
		}

		ctx := context.WithValue(context.Background(), "eid", uuid.NewString())
		if repos, err := manager.ListRepositories(ctx); err != nil {
			return err
		} else {
			hasRepo := slices.ContainsFunc(repos, func(r store.CachedRepository) bool {
				return r.RepositorySource == operatorhubio
			})
			if hasRepo {
				return nil
			}
		}
		// set to debug level to see what's happening
		lgr.SetLevel(logrus.DebugLevel)
		lgr.Info("loading operatorhub.io repository")
		if err := manager.AddRepository(context.Background(), operatorhubio); err != nil {
			return err
		}
		defer manager.Close()
		lgr.Info("done")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(setupCmd)
}
