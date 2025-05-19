package cmd

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"log/slog"
	"main.go/app"
	"main.go/client"
	"main.go/config"
	"main.go/cqrs"
	"main.go/db"
	"main.go/kafka"
	"main.go/otel"
	"os"
	"testing"
)

func TestImport(t *testing.T) {
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	var user1 int64 = 1
	chat1Name := "new chat 1"
	message1Text := "new message 1"

	var message1Id int64
	var chat1Id int64

	resetInfra(slogLogger)

	// fill with 1 chat and 1 message
	runTestFunc(slogLogger, t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		ctx := context.Background()

		var err error
		chat1Id, err = restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)

		message1Id, err = restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")

		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in char participants")
		assert.Equal(t, []int64{user1}, chat1Participants)

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
	})

	slogLogger.Info("Start export command")
	appExportFx := fx.New(
		fx.Supply(slogLogger),
		fx.Provide(
			config.CreateTestTypedConfig,
			kafka.ConfigureSaramaClient,
		),
		fx.Invoke(
			kafka.Export,
			app.Shutdown,
		),
	)
	appExportFx.Run()
	slogLogger.Info("Exit export command")

	resetInfra(slogLogger)

	slogLogger.Info("Start import command")
	appImportFx := fx.New(
		fx.Supply(slogLogger),
		fx.Provide(
			config.CreateTestTypedConfig,
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureCommonProjection,
		),
		fx.Invoke(
			db.RunMigrations,
			kafka.RunCreateTopic,
			kafka.Import,
			cqrs.SetIsNeedToFastForwardSequences,
			app.Shutdown,
		),
	)
	appImportFx.Run()
	slogLogger.Info("Exit import command")

	runTestFunc(slogLogger, t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		ctx := context.Background()

		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in char participants")
		assert.Equal(t, []int64{user1}, chat1Participants)

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
	})
}
