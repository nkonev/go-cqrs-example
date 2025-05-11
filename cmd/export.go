/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"log/slog"
	"main.go/app"
	"main.go/config"
	"main.go/kafka"
	"main.go/otel"
	"os"
)

// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export events",
	Long:  `Export events from configured topic to stdout.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunExport()
	},
}

func init() {
	rootCmd.AddCommand(exportCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// exportCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// exportCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func RunExport() {
	slogLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	slogLogger.Info("Start export command")

	appFx := fx.New(
		fx.Supply(slogLogger),
		fx.Provide(
			config.CreateTypedConfig,
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			kafka.ConfigureSaramaClient,
		),
		fx.Invoke(
			kafka.Export,
			app.Shutdown,
		),
	)
	appFx.Run()
	slogLogger.Info("Exit export command")
}
