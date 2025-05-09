package internal

import "time"

type AdditionalData struct {
	CreatedAt time.Time `json:"createdAt"`
	Partition int32     `json:"-"` // set on receive
	Offset    int64     `json:"-"` // set on receive
}

type ChatCreated struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Title          string          `json:"title"`
}

type ParticipantsAdded struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantIds []int64         `json:"participantIds"`
	ChatId         int64           `json:"chatId"`
}

type ParticipantRemoved struct {
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
	ParticipantIds []int64         `json:"participantIds"`
	ChatId         int64           `json:"chatId"`
	IncreaseOn     int             `json:"increaseOn"`
	MessageOwnerId int64           `json:"messageOwnerId"`
}

type UnreadMessageRefreshed struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantIds []int64         `json:"participantIds"`
	ChatId         int64           `json:"chatId"`
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
	return ToString(s.ChatId)
}

func (s *ParticipantsAdded) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *ParticipantRemoved) GetPartitionKey() string {
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

func (s *MessageRemoved) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *UnreadMessageRefreshed) GetPartitionKey() string {
	return ToString(s.ChatId)
}

func (s *ChatCreated) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *ParticipantsAdded) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *ParticipantRemoved) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *ChatPinned) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *MessageCreated) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *UnreadMessageIncreased) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *MessageReaded) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *MessageRemoved) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *UnreadMessageRefreshed) SetOffset(partition int32, offset int64) {
	s.AdditionalData.Partition = partition
	s.AdditionalData.Offset = offset
}

func (s *ChatCreated) Name() string {
	return "chatCreated"
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

func (s *UnreadMessageIncreased) Name() string {
	return "unreadMessageIncreased"
}

func (s *MessageReaded) Name() string {
	return "messageReaded"
}

func (s *MessageRemoved) Name() string {
	return "messageRemoved"
}

func (s *UnreadMessageRefreshed) Name() string {
	return "unreadMessageRefreshed"
}
