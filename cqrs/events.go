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

type ChatRemoved struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
}

type ParticipantsAdded struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantIds []int64         `json:"participantIds"`
	ChatId         int64           `json:"chatId"`
}

type ParticipantRemoved struct {
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

type MessageRemoved struct {
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

func (s *ChatRemoved) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantsAdded) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantRemoved) GetPartitionKey() string {
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

func (s *MessageRemoved) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatCreated) Name() string {
	return "chatCreated"
}

func (s *ChatEdited) Name() string {
	return "chatEdited"
}

func (s *ChatRemoved) Name() string {
	return "chatRemoved"
}

func (s *ParticipantsAdded) Name() string {
	return "participantsAdded"
}

func (s *ParticipantRemoved) Name() string {
	return "participantRemoved"
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

func (s *MessageRemoved) Name() string {
	return "messageRemoved"
}
