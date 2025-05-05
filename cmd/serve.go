/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"log/slog"
	"main.go/internal"
	"os"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start server",
	Long:  `Start http server and CQRS infrastructure`,
	Run: func(cmd *cobra.Command, args []string) {
		RunServe()
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// serveCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// serveCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func RunServe() {
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	slogLogger.Info("Start serve command")

	appFx := fx.New(
		fx.Supply(slogLogger),
		fx.Provide(
			internal.CreateTypedConfig,
			internal.ConfigureTracePropagator,
			internal.ConfigureTraceProvider,
			internal.ConfigureTraceExporter,
			internal.ConfigureDatabase,
			internal.ConfigureKafkaAdmin,
			internal.ConfigureKafkaMarshaller,
			internal.ConfigureWatermillLogger,
			internal.ConfigurePublisher,
			internal.ConfigureCqrsRouter,
			internal.ConfigureCqrsMarshaller,
			internal.ConfigureEventBus,
			internal.ConfigureEventProcessor,
			internal.ConfigureCommonProjection,
			internal.ConfigureHttpServer,
			internal.ConfigureSaramaClient,
		),
		fx.Invoke(
			internal.RunMigrations,
			internal.RunCreateTopic,
			internal.RunHttpServer,
			internal.RunCqrsRouter,
			internal.RunSequenceFastforwarder,
		),
	)
	appFx.Run()
	slogLogger.Info("Exit serve command")
}
