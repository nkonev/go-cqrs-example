package cmd

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/kafka"
	"go.uber.org/fx"
	"log/slog"
	"testing"
)

func TestUnreads(t *testing.T) {
	startAppFull(t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
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
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		message1Text := "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

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
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
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
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

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
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

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
		assert.NoError(t, err, "error in delete message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

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

func TestPinChat(t *testing.T) {
	startAppFull(t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		var user1 int64 = 1
		var user2 int64 = 2

		chat1Name := "new chat 1"
		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		message1Text := "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = restClient.PinChat(ctx, user1, chat1Id, true)
		assert.NoError(t, err, "error in pinning chats")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, true, chat1OfUser1New2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser2New2 := user2ChatsNew2[0]
		assert.Equal(t, false, chat1OfUser2New2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2New2.Title)
		assert.Equal(t, int64(1), chat1OfUser2New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2New2.LastMessageContent)
	})

}

func TestDeleteChat(t *testing.T) {
	startAppFull(t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		var user1 int64 = 1
		var user2 int64 = 2

		chat1Name := "new chat 1"
		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		message1Text := "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = restClient.DeleteChat(ctx, chat1Id)
		assert.NoError(t, err, "error in removing chats")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user1ChatsNew2))

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2ChatsNew2))
	})

}

func TestAddParticipant(t *testing.T) {
	startAppFull(t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		var user1 int64 = 1
		var user2 int64 = 2

		chat1Name := "new chat 1"
		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		message1Text := "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2.LastMessageContent)

		chat1NewName := "new chat 1 renamed"
		err = restClient.EditChat(ctx, user1, chat1NewName)
		assert.NoError(t, err, "error in changing chat")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, chat1NewName, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser2New2 := user2ChatsNew2[0]
		assert.Equal(t, chat1NewName, chat1OfUser2New2.Title)
		assert.Equal(t, int64(1), chat1OfUser2New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2New2.LastMessageContent)
	})
}

func TestDeleteParticipant(t *testing.T) {
	startAppFull(t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		var user1 int64 = 1
		var user2 int64 = 2

		chat1Name := "new chat 1"
		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		message1Text := "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2.LastMessageContent)

		err = restClient.DeleteChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in removing chat participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2ChatsNew2))

		chat1Participants2, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1}, chat1Participants2)
	})
}

func TestEditMessage(t *testing.T) {
	startAppFull(t, func(
		slogLogger *slog.Logger,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		var user1 int64 = 1

		chat1Name := "new chat 1"
		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		message1Text := "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, message1Text, *chat1OfUser1.LastMessageContent)

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		message1TextNew := "new message 1 edited"

		err = restClient.EditMessage(ctx, user1, chat1Id, message1.Id, message1TextNew)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(slogLogger, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew, err := restClient.GetChatsByUserId(ctx, user1)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew))
		chat1OfUser1New := user1ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser1New.Title)
		assert.Equal(t, int64(0), chat1OfUser1New.UnreadMessages)
		assert.Equal(t, message1TextNew, *chat1OfUser1New.LastMessageContent)

		chat1MessagesNew, err := restClient.GetMessages(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1MessagesNew))
		message1Mew := chat1MessagesNew[0]
		assert.Equal(t, message1Id, message1Mew.Id)
		assert.Equal(t, message1TextNew, message1Mew.Content)
	})
}
