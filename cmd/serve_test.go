package cmd

import (
	"context"
	"github.com/IBM/sarama"
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

	var s fx.Shutdowner
	appTestFx := fxtest.New(
		t,
		fx.Supply(
			slogLogger,
		),
		fx.Populate(&s),
		fx.Provide(
			config.CreateTypedConfig,
			kafka.ConfigureFastTestCurrentOffsetsStore,
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
			WaitForHealthCheck,
			testFunc,
		),
	)
	defer appTestFx.RequireStart().RequireStop()
	assert.NoError(t, s.Shutdown(), "error in app shutdown")
	return appTestFx
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
	startAppFull(t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		fastTestCurrentOffsetsStore *kafka.FastTestCurrentOffsetsStore,
		lc fx.Lifecycle,
	) {
		var user1 int64 = 1
		var user2 int64 = 2
		var user3 int64 = 3

		chat1Name := "new chat 1"
		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessedTest(slogLogger, cfg, saramaClient, fastTestCurrentOffsetsStore, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		message1Text := "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessedTest(slogLogger, cfg, saramaClient, fastTestCurrentOffsetsStore, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		user3Chats, err := restClient.GetChatsByUserId(ctx, user3)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user3Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2, user3})
		assert.NoError(t, kafka.WaitForAllEventsProcessedTest(slogLogger, cfg, saramaClient, fastTestCurrentOffsetsStore, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in char participants")
		assert.Equal(t, []int64{user1, user2, user3}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		user3ChatsNew, err := restClient.GetChatsByUserId(ctx, user3)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew))
		chat1OfUser3 := user3ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser3.Title)
		assert.Equal(t, int64(1), chat1OfUser3.UnreadMessages)

		err = restClient.ReadMessage(ctx, user2, chat1Id, message1.Id)
		assert.NoError(t, err, "error in reading message")
		assert.NoError(t, kafka.WaitForAllEventsProcessedTest(slogLogger, cfg, saramaClient, fastTestCurrentOffsetsStore, lc), "error in waiting for processing events")

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser22 := user2ChatsNew2[0]
		assert.Equal(t, int64(0), chat1OfUser22.UnreadMessages)

		user3ChatsNew2, err := restClient.GetChatsByUserId(ctx, user3)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew2))
		chat1OfUser32 := user3ChatsNew2[0]
		assert.Equal(t, int64(1), chat1OfUser32.UnreadMessages)

		message2Text := "new message 2"
		_, err = restClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		assert.NoError(t, err, "error in creating message")

		message3Text := "new message 2"
		messageId3, err := restClient.CreateMessage(ctx, user1, chat1Id, message3Text)
		assert.NoError(t, err, "error in creating message")
		assert.True(t, messageId3 > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessedTest(slogLogger, cfg, saramaClient, fastTestCurrentOffsetsStore, lc), "error in waiting for processing events")

		user2ChatsNew3, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew3))
		chat1OfUser23 := user2ChatsNew3[0]
		assert.Equal(t, int64(2), chat1OfUser23.UnreadMessages)

		user3ChatsNew3, err := restClient.GetChatsByUserId(ctx, user3)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew3))
		chat1OfUser33 := user3ChatsNew3[0]
		assert.Equal(t, int64(3), chat1OfUser33.UnreadMessages)

		err = restClient.DeleteMessage(ctx, user1, chat1Id, messageId3)
		assert.NoError(t, err, "error in remove message")
		assert.NoError(t, kafka.WaitForAllEventsProcessedTest(slogLogger, cfg, saramaClient, fastTestCurrentOffsetsStore, lc), "error in waiting for processing events")

		user2ChatsNew4, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew4))
		chat1OfUser24 := user2ChatsNew4[0]
		assert.Equal(t, int64(1), chat1OfUser24.UnreadMessages)

		user3ChatsNew4, err := restClient.GetChatsByUserId(ctx, user3)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew4))
		chat1OfUser34 := user3ChatsNew4[0]
		assert.Equal(t, int64(2), chat1OfUser34.UnreadMessages)
	})

}
