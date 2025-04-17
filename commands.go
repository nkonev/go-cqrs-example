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

type ChatPin struct {
	AdditionalData *AdditionalData
	ChatId         int64
	Pin            bool
	ParticipantId  int64
}

func (s *ChatCreate) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection) (int64, error) {
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

func (s *ChatPin) Handle(ctx context.Context, eventBus EventBusInterface) error {
	cp := &ChatPinned{
		AdditionalData: s.AdditionalData,
		ParticipantId:  s.ParticipantId,
		ChatId:         s.ChatId,
		Pinned:         s.Pin,
	}
	return eventBus.Publish(ctx, cp)
}
