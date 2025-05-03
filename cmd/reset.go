/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"go.uber.org/fx"
	"log/slog"
	"main.go/internal"
	"os"

	"github.com/spf13/cobra"
)

// resetCmd represents the reset command
var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunReset()
	},
}

func init() {
	rootCmd.AddCommand(resetCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// resetCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// resetCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func RunReset() {
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	slogLogger.Info("Start reset command")

	appFx := fx.New(
		fx.Supply(slogLogger),
		fx.Provide(
			internal.CreateTypedConfig,
			internal.ConfigureTracePropagator,
			internal.ConfigureTraceProvider,
			internal.ConfigureTraceExporter,
			internal.ConfigureDatabase,
			internal.ConfigureKafkaAdmin,
		),
		fx.Invoke(
			internal.RunResetDatabase,
			internal.RunResetPartitions,
			internal.RunMigrations,
			internal.RunCreateTopic,
			internal.Shutdown,
		),
	)
	appFx.Run()
	slogLogger.Info("Exit reset command")
}
