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
	return ToString(s.ChatId) // TODO think about userId partitioning
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
