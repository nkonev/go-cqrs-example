package cmd

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"log/slog"
	"main.go/client"
	"main.go/config"
	"main.go/cqrs"
	"main.go/db"
	"main.go/handlers"
	"main.go/kafka"
	"main.go/otel"
	"main.go/utils"
	"net/http"
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

func startAppFull(t *testing.T, testFunc interface{}) *fxtest.App {
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	var s fx.Shutdowner
	appFx := fxtest.New(
		t,
		fx.Supply(
			slogLogger,
		),
		fx.Populate(&s),
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
			client.NewRestClient,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunDeleteTopic,
			db.RunMigrations,
			kafka.RunCreateTopic,
			cqrs.RunCqrsRouter,
			handlers.RunHttpServer,
			WaitForHealthCheck,
			testFunc,
		),
	)
	defer appFx.RequireStart().RequireStop()
	assert.NoError(t, s.Shutdown(), "error in app shutdown")
	return appFx
}

func WaitForHealthCheck(slogLogger *slog.Logger, restClient *client.RestClient, cfg *config.AppConfig) {
	defer restClient.CloseIdleConnections()
	i := 0
	const maxAttempts = 60
	success := false
	for ; i <= maxAttempts; i++ {
		requestHeaders1 := map[string][]string{}
		getChatRequest := &http.Request{
			Method: "GET",
			Header: requestHeaders1,
			URL:    utils.StringToUrl("http://localhost" + cfg.HttpServerConfig.Address + "/internal/health"),
		}
		getChatResponse, err := restClient.Do(getChatRequest)
		if err != nil {
			slogLogger.Info("Awaiting while chat have been started - transport error")
			time.Sleep(time.Second * 1)
			continue
		} else if !(getChatResponse.StatusCode >= 200 && getChatResponse.StatusCode < 300) {
			slogLogger.Info("Awaiting while chat have been started - non-2xx code")
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

func TestUnreads(t *testing.T) {
	startAppFull(t, func(slogLogger *slog.Logger, cfg *config.AppConfig, restClient *client.RestClient, saramaClient sarama.Client, m *cqrs.CommonProjection, lc fx.Lifecycle) {
		var user1 int64 = 1
		chatName := "chat 1"
		ctx := context.Background()

		chatId, err := restClient.CreateChat(ctx, user1, chatName)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chatId > 0)

		err = kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc)
		assert.NoError(t, err, "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chatId)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chatName, title)
	})

}
