package main

import (
	"context"
	"errors"
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

func (s *ChatCreate) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection) (int64, error) {
	err := commonProjection.InitializeChatIdSequenceIfNeed(ctx)
	if err != nil {
		return 0, err
	}

	chatId, err := commonProjection.GetNextChatId(ctx)
	if err != nil {
		return 0, err
	}

	cc := &ChatCreated{
		AdditionalData: s.AdditionalData,
		ChatId:         chatId,
		Title:          s.Title,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return 0, err
	}

	addParticipantErrors := []error{}
	for _, participantId := range s.ParticipantIds {
		pa := &ParticipantAdded{
			AdditionalData: s.AdditionalData,
			ParticipantId:  participantId,
			ChatId:         chatId,
		}
		err = eventBus.Publish(ctx, pa)
		if err != nil {
			addParticipantErrors = append(addParticipantErrors, err)
		}
	}

	if len(addParticipantErrors) > 0 {
		return 0, errors.Join(addParticipantErrors...)
	}

	return chatId, nil
}

func (s *ParticipantAdd) Handle(ctx context.Context, eventBus EventBusInterface) error {
	addParticipantErrors := []error{}
	for _, participantId := range s.ParticipantIds {
		pa := &ParticipantAdded{
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

func (s *MessagePost) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection) (int64, error) {
	err := commonProjection.InitializeMessageIdSequenceIfNeed(ctx, s.ChatId)
	if err != nil {
		return 0, err
	}

	messageId, err := commonProjection.GetNextMessageId(ctx, s.ChatId)
	if err != nil {
		return 0, err
	}

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

	increaseUnreadMessagesErrors := []error{}
	for _, participantId := range participantIds {
		ui := &UnreadMessageIncreased{
			AdditionalData: s.AdditionalData,
			ParticipantId:  participantId,
			ChatId:         s.ChatId,
			IncreaseOn:     1,
		}
		err = eventBus.Publish(ctx, ui)
		if err != nil {
			increaseUnreadMessagesErrors = append(increaseUnreadMessagesErrors, err)
		}
	}
	if len(increaseUnreadMessagesErrors) > 0 {
		return 0, errors.Join(increaseUnreadMessagesErrors...)
	}

	return messageId, err
}

func (s *MessageRead) Handle(ctx context.Context, eventBus EventBusInterface) error {
	cp := &MessageReaded{
		AdditionalData: s.AdditionalData,
		ParticipantId:  s.ParticipantId,
		ChatId:         s.ChatId,
		MessageId:      s.MessageId,
	}
	return eventBus.Publish(ctx, cp)
}
