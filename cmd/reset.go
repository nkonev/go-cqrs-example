/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"go.uber.org/fx"
	"log/slog"
	"main.go/app"
	"main.go/config"
	"main.go/cqrs"
	"main.go/db"
	"main.go/kafka"
	"main.go/otel"
	"os"

	"github.com/spf13/cobra"
)

// resetCmd represents the reset command
var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset offsets and storage",
	Long:  `Reset offsets in Kafka for configured topic and consumer group, drops all the tables, sequences from PostgreSQL and creates empty ones with help of migration.`,
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
			config.CreateTypedConfig,
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureCommonProjection,
			kafka.ConfigureSaramaClient,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunResetPartitions,
			db.RunMigrations,
			kafka.RunCreateTopic,
			cqrs.SetIsNeedToFastForwardSequences,
			app.Shutdown,
		),
	)
	appFx.Run()
	slogLogger.Info("Exit reset command")
}
