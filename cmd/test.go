package cmd

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"log/slog"
	"main.go/app"
	"main.go/client"
	"main.go/config"
	"main.go/cqrs"
	"main.go/db"
	"main.go/handlers"
	"main.go/kafka"
	"main.go/otel"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	setup()
	retCode := m.Run()
	shutdown()
	os.Exit(retCode)
}

func setup() {

}

func shutdown() {

}

func resetInfra(slogLogger *slog.Logger) {
	appFx := fx.New(
		fx.Supply(
			slogLogger,
		),
		fx.Provide(
			config.CreateTypedConfig,
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunDeleteTopic,
			db.RunMigrations,
			kafka.RunCreateTopic,
			app.Shutdown,
		),
	)
	appFx.Run()
}

func runTestFunc(slogLogger *slog.Logger, t *testing.T, testFunc interface{}) {
	var s fx.Shutdowner
	appTestFx := fxtest.New(
		t,
		fx.Supply(
			slogLogger,
		),
		fx.Populate(&s),
		fx.Provide(
			config.CreateTestTypedConfig,
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
			client.NewRestClient,
		),
		fx.Invoke(
			cqrs.RunCqrsRouter,
			handlers.RunHttpServer,
			waitForHealthCheck,
			testFunc,
		),
	)
	defer appTestFx.RequireStart().RequireStop()
	assert.NoError(t, s.Shutdown(), "error in app shutdown")
}

func startAppFull(t *testing.T, testFunc interface{}) {
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	resetInfra(slogLogger)

	runTestFunc(slogLogger, t, testFunc)
}

func waitForHealthCheck(slogLogger *slog.Logger, restClient *client.RestClient, cfg *config.AppConfig) {
	ctx := context.Background()

	i := 0
	const maxAttempts = 60
	success := false
	for ; i <= maxAttempts; i++ {
		err := restClient.HealthCheck(ctx)
		if err != nil {
			slogLogger.Info("Awaiting while chat have been started")
			time.Sleep(time.Second * 1)
			continue
		} else {
			success = true
			break
		}
	}
	if !success {
		panic("Cannot await for chat will be started")
	}
	slogLogger.Info("chat have started")
}
