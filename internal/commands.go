package internal

import (
	"context"
	"errors"
	"fmt"
)

type ChatCreate struct {
	AdditionalData *AdditionalData
	Title          string
	ParticipantIds []int64
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

func (s *ChatCreate) Handle(ctx context.Context, eventBus EventBusInterface, db *DB, commonProjection *CommonProjection) (int64, error) {
	chatId, err := TransactWithResult(ctx, db, func(tx *Tx) (int64, error) {
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

func (s *ParticipantAdd) Handle(ctx context.Context, eventBus EventBusInterface) error {
	pa := &ParticipantsAdded{
		AdditionalData: s.AdditionalData,
		ParticipantIds: s.ParticipantIds,
		ChatId:         s.ChatId,
	}
	return eventBus.Publish(ctx, pa)
}

// TODO batching
func (s *ParticipantRemove) Handle(ctx context.Context, eventBus EventBusInterface) error {
	addParticipantErrors := []error{}
	for _, participantId := range s.ParticipantIds {
		pa := &ParticipantRemoved{
			AdditionalData: s.AdditionalData,
			ParticipantId:  participantId,
			ChatId:         s.ChatId,
		}
		err := eventBus.Publish(ctx, pa)
		if err != nil {
			addParticipantErrors = append(addParticipantErrors, err)
		}
	}

	if len(addParticipantErrors) > 0 {
		return errors.Join(addParticipantErrors...)
	}

	return nil
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

func (s *MessagePost) Handle(ctx context.Context, eventBus EventBusInterface, db *DB, commonProjection *CommonProjection) (int64, error) {
	messageId, err := TransactWithResult(ctx, db, func(tx *Tx) (int64, error) {
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

	ui := &UnreadMessageIncreased{
		AdditionalData: s.AdditionalData,
		ParticipantIds: participantIds,
		ChatId:         s.ChatId,
		IncreaseOn:     1,
		MessageOwnerId: s.OwnerId,
	}

	err = eventBus.Publish(ctx, ui)
	if err != nil {
		return 0, err
	}

	return messageId, err
}

func (s *MessageRead) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection) error {

	has, err := commonProjection.HasUnreadMessagesInChat(ctx, s.ChatId, s.ParticipantId)
	if err != nil {
		return err
	}

	if !has {
		return nil
	}

	cp := &MessageReaded{
		AdditionalData: s.AdditionalData,
		ParticipantId:  s.ParticipantId,
		ChatId:         s.ChatId,
		MessageId:      s.MessageId,
	}
	return eventBus.Publish(ctx, cp)
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

	ui := &UnreadMessageRefreshed{
		AdditionalData: s.AdditionalData,
		ParticipantIds: participantIds,
		ChatId:         s.ChatId,
	}
	return eventBus.Publish(ctx, ui)
}
