package main

import "time"

type AdditionalData struct {
	CreatedAt time.Time `json:"createdAt"`
}

type ChatCreated struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Title          string          `json:"title"`
}

type ParticipantAdded struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
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

type UnreadMessageIncreased struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	IncreaseOn     int             `json:"increaseOn"`
}

type MessageReaded struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
}

func GenerateMessageAdditionalData() *AdditionalData {
	return &AdditionalData{
		CreatedAt: time.Now().UTC(),
	}
}

func (s *ChatCreated) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *ParticipantAdded) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *ChatPinned) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *MessageCreated) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *UnreadMessageIncreased) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *MessageReaded) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *ChatCreated) Name() string {
	return "chatCreated"
}

func (s *ParticipantAdded) Name() string {
	return "participantAdded"
}

func (s *ChatPinned) Name() string {
	return "chatPinned"
}

func (s *MessageCreated) Name() string {
	return "messageCreated"
}

func (s *UnreadMessageIncreased) Name() string {
	return "unreadMessageIncreased"
}

func (s *MessageReaded) Name() string {
	return "messageReaded"
}
