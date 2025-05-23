package cqrs

import (
	"go-cqrs-chat-example/utils"
	"time"
)

type AdditionalData struct {
	CreatedAt time.Time `json:"createdAt"`
}

type ChatCreated struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Title          string          `json:"title"`
}

type ChatEdited struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Title          string          `json:"title"`
}

type ChatDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
}

type ParticipantsAdded struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantIds []int64         `json:"participantIds"`
	ChatId         int64           `json:"chatId"`
}

type ParticipantDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantIds []int64         `json:"participantIds"`
	ChatId         int64           `json:"chatId"`
}

type ChatPinned struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	Pinned         bool            `json:"pinned"`
}

type MessageCreated struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Id             int64           `json:"id"`
	OwnerId        int64           `json:"ownerId"`
	ChatId         int64           `json:"chatId"`
	Content        string          `json:"content"`
}

type UnreadMessagesAction int16

const (
	UnreadMessagesActionRefresh = iota + 1
	UnreadMessagesActionIncrease
)

type LastMessageAction int16

const (
	LastMessageActionRefresh = iota + 1
)

type ChatCommonAction int16

const (
	ChatCommonActionRefresh = iota + 1
)

type ChatViewRefreshed struct {
	AdditionalData       *AdditionalData      `json:"additionalData"`
	ParticipantIds       []int64              `json:"participantIds"`
	ChatId               int64                `json:"chatId"`
	Title                string               `json:"title"` // chat title
	UnreadMessagesAction UnreadMessagesAction `json:"unreadMessagesAction"`
	LastMessageAction    LastMessageAction    `json:"lastMessageAction"`
	ChatCommonAction     ChatCommonAction     `json:"chatCommonAction"`
	IncreaseOn           int                  `json:"increaseOn"`
	OwnerId              int64                `json:"ownerId"` // owner of message
}

type MessageReaded struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
}

type MessageDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
}

func GenerateMessageAdditionalData() *AdditionalData {
	return &AdditionalData{
		CreatedAt: time.Now().UTC(),
	}
}

func (s *ChatCreated) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatEdited) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantsAdded) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatPinned) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageCreated) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatViewRefreshed) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageReaded) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatCreated) Name() string {
	return "chatCreated"
}

func (s *ChatEdited) Name() string {
	return "chatEdited"
}

func (s *ChatDeleted) Name() string {
	return "chatDeleted"
}

func (s *ParticipantsAdded) Name() string {
	return "participantsAdded"
}

func (s *ParticipantDeleted) Name() string {
	return "participantDeleted"
}

func (s *ChatPinned) Name() string {
	return "chatPinned"
}

func (s *MessageCreated) Name() string {
	return "messageCreated"
}

func (s *ChatViewRefreshed) Name() string {
	return "chatViewRefreshed"
}

func (s *MessageReaded) Name() string {
	return "messageReaded"
}

func (s *MessageDeleted) Name() string {
	return "messageDeleted"
}
