/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"log/slog"
	"main.go/config"
	"main.go/cqrs"
	"main.go/db"
	"main.go/handlers"
	"main.go/kafka"
	"main.go/otel"
	"os"
	"strings"
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
	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == "level" {
				return slog.Attr{
					Key:   "level",
					Value: slog.StringValue(strings.ToLower(a.Value.String())),
				}
			} else {
				return a
			}
		},
	}))

	slogLogger.Info("Start serve command")

	appFx := fx.New(
		fx.Supply(slogLogger),
		fx.Provide(
			config.CreateTypedConfig,
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureKafkaMarshaller,
			cqrs.ConfigureWatermillLogger,
			cqrs.ConfigurePublisher,
			cqrs.ConfigureCqrsRouter,
			cqrs.ConfigureCqrsMarshaller,
			cqrs.ConfigureEventBus,
			cqrs.ConfigureEventProcessor,
			cqrs.ConfigureCommonProjection,
			handlers.ConfigureHttpServer,
			kafka.ConfigureSaramaClient,
		),
		fx.Invoke(
			db.RunMigrations,
			kafka.RunCreateTopic,
			cqrs.RunCqrsRouter,
			kafka.WaitForAllEventsProcessed,
			cqrs.RunSequenceFastforwarder,
			handlers.RunHttpServer,
		),
	)
	appFx.Run()
	slogLogger.Info("Exit serve command")
}
