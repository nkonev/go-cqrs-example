package cqrs

import (
	"context"
	"fmt"
	"main.go/db"
)

type ChatCreate struct {
	AdditionalData *AdditionalData
	Title          string
	ParticipantIds []int64
}

type ChatEdit struct {
	ChatId              int64
	AdditionalData      *AdditionalData
	Title               string
	ParticipantIdsToAdd []int64
}

type ParticipantAdd struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
}

type ParticipantRemove struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
}

type MessageRemove struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
}

type ChatPin struct {
	AdditionalData *AdditionalData
	ChatId         int64
	Pin            bool
	ParticipantId  int64
}

type MessagePost struct {
	AdditionalData *AdditionalData
	ChatId         int64
	OwnerId        int64
	Content        string
}

type MessageRead struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
	ParticipantId  int64
}

func (s *ChatCreate) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) (int64, error) {
	chatId, err := db.TransactWithResult(ctx, dba, func(tx *db.Tx) (int64, error) {
		return commonProjection.GetNextChatId(ctx, tx)
	})

	cc := &ChatCreated{
		AdditionalData: s.AdditionalData,
		ChatId:         chatId,
		Title:          s.Title,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return 0, err
	}

	pa := &ParticipantsAdded{
		AdditionalData: s.AdditionalData,
		ParticipantIds: s.ParticipantIds,
		ChatId:         chatId,
	}
	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return 0, err
	}

	return chatId, nil
}

func (s *ChatEdit) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection) error {
	cc := &ChatEdited{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		Title:          s.Title,
	}
	err := eventBus.Publish(ctx, cc)
	if err != nil {
		return err
	}

	if len(s.ParticipantIdsToAdd) > 0 {
		pa := &ParticipantsAdded{
			AdditionalData: s.AdditionalData,
			ParticipantIds: s.ParticipantIdsToAdd,
			ChatId:         s.ChatId,
		}
		err = eventBus.Publish(ctx, pa)
		if err != nil {
			return err
		}
	}

	participantIds, err := commonProjection.GetParticipants(ctx, s.ChatId)
	if err != nil {
		return err
	}

	ui := &ChatViewRefreshed{
		AdditionalData:   s.AdditionalData,
		ParticipantIds:   participantIds,
		ChatId:           s.ChatId,
		ChatCommonAction: ChatCommonActionRefresh,
		Title:            s.Title,
	}

	err = eventBus.Publish(ctx, ui)
	if err != nil {
		return err
	}

	return nil
}

func (s *ParticipantAdd) Handle(ctx context.Context, eventBus EventBusInterface) error {
	pa := &ParticipantsAdded{
		AdditionalData: s.AdditionalData,
		ParticipantIds: s.ParticipantIds,
		ChatId:         s.ChatId,
	}
	return eventBus.Publish(ctx, pa)
}

func (s *ParticipantRemove) Handle(ctx context.Context, eventBus EventBusInterface) error {
	pa := &ParticipantRemoved{
		AdditionalData: s.AdditionalData,
		ParticipantIds: s.ParticipantIds,
		ChatId:         s.ChatId,
	}
	return eventBus.Publish(ctx, pa)
}

func (s *ChatPin) Handle(ctx context.Context, eventBus EventBusInterface) error {
	cp := &ChatPinned{
		AdditionalData: s.AdditionalData,
		ParticipantId:  s.ParticipantId,
		ChatId:         s.ChatId,
		Pinned:         s.Pin,
	}
	return eventBus.Publish(ctx, cp)
}

func (s *MessagePost) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) (int64, error) {
	messageId, err := db.TransactWithResult(ctx, dba, func(tx *db.Tx) (int64, error) {
		return commonProjection.GetNextMessageId(ctx, tx, s.ChatId)
	})

	mc := &MessageCreated{
		AdditionalData: s.AdditionalData,
		Id:             messageId,
		OwnerId:        s.OwnerId,
		ChatId:         s.ChatId,
		Content:        s.Content,
	}

	err = eventBus.Publish(ctx, mc)
	if err != nil {
		return 0, err
	}

	participantIds, err := commonProjection.GetParticipants(ctx, s.ChatId)
	if err != nil {
		return 0, err
	}

	ui := &ChatViewRefreshed{
		AdditionalData:       s.AdditionalData,
		ParticipantIds:       participantIds,
		ChatId:               s.ChatId,
		UnreadMessagesAction: UnreadMessagesActionIncrease,
		IncreaseOn:           1,
		OwnerId:              s.OwnerId,
		LastMessageAction:    LastMessageActionRefresh,
	}

	err = eventBus.Publish(ctx, ui)
	if err != nil {
		return 0, err
	}

	return messageId, err
}

func (s *MessageRead) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection) error {

	lastMessageReadedId, lastMessgeReadedExists, maxMessageId, err := commonProjection.GetLastMessageReaded(ctx, s.ChatId, s.ParticipantId)
	if err != nil {
		return err
	}

	messageIdToMark := s.MessageId

	if s.MessageId > maxMessageId {
		messageIdToMark = maxMessageId
	}

	if (lastMessgeReadedExists && messageIdToMark > lastMessageReadedId) || (!lastMessgeReadedExists && lastMessageReadedId == 0) {
		cp := &MessageReaded{
			AdditionalData: s.AdditionalData,
			ParticipantId:  s.ParticipantId,
			ChatId:         s.ChatId,
			MessageId:      messageIdToMark,
		}
		return eventBus.Publish(ctx, cp)
	}

	return nil
}

func (s *MessageRemove) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection, userId int64) error {

	ownerId, err := commonProjection.GetMessageOwner(ctx, s.ChatId, s.MessageId)
	if err != nil {
		return err
	}

	if ownerId != userId {
		return fmt.Errorf("User %v is not an owner of message %v in chat %v", userId, s.MessageId, s.ChatId)
	}

	cp := &MessageRemoved{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		MessageId:      s.MessageId,
	}
	err = eventBus.Publish(ctx, cp)
	if err != nil {
		return err
	}

	participantIds, err := commonProjection.GetParticipants(ctx, s.ChatId)
	if err != nil {
		return err
	}

	ui := &ChatViewRefreshed{
		AdditionalData:       s.AdditionalData,
		ParticipantIds:       participantIds,
		ChatId:               s.ChatId,
		UnreadMessagesAction: UnreadMessagesActionRefresh,
		OwnerId:              userId,
		LastMessageAction:    LastMessageActionRefresh,
	}

	err = eventBus.Publish(ctx, ui)
	if err != nil {
		return err
	}
	return nil
}
