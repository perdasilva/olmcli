package cmd

import (
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"log/slog"
	"os"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "dpz",
	Short: "Interacts with the deppy solver and OLM pipeline",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		debug, _ := cmd.Flags().GetBool("debug")
		level := slog.LevelInfo
		if debug {
			level = slog.LevelDebug
		}
		opts := &slog.HandlerOptions{
			Level: level,
		}
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, opts)).With("app", "dpz", "id", uuid.NewString()))
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		pkgToInstall, err := cmd.Flags().GetStringSlice("install")
		if err != nil {
			return err
		}
		baseline, err := cmd.Flags().GetStringSlice("baseline")
		if err != nil {
			return err
		}
		runSolver(pkgToInstall, baseline)
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	// setup logger and log format
	err := rootCmd.Execute()
	if err != nil {
		slog.Error("execution error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolP("debug", "d", false, "set debug output level")
	rootCmd.Flags().StringSliceP("install", "i", []string{}, "list of packages to install. Format <name>[:<version range>], e.g. prometheus or prometheus:>0.37.0")
	rootCmd.Flags().StringSliceP("baseline", "b", []string{}, "list of installed bundles. Format <name>[:<version>], e.g. prometheus or prometheus:0.37.0")
}
