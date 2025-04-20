package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

type CommonProjection struct {
	db         *sql.DB
	slogLogger *slog.Logger
}

func NewCommonProjection(db *sql.DB, slogLogger *slog.Logger) *CommonProjection {
	return &CommonProjection{
		db:         db,
		slogLogger: slogLogger,
	}
}

func (m *CommonProjection) GetNextChatId(ctx context.Context) (int64, error) {
	r := m.db.QueryRowContext(ctx, "select nextval('chat_id_sequence')")
	if r.Err() != nil {
		return 0, r.Err()
	}
	var nid int64
	err := r.Scan(&nid)
	if err != nil {
		return 0, err
	}
	return nid, nil
}

func (m *CommonProjection) GetNextMessageId(ctx context.Context, chatId int64) (int64, error) {
	var messageId int64
	res := m.db.QueryRowContext(ctx, "UPDATE chat_common SET last_generated_message_id = last_generated_message_id + 1 WHERE id = $1 RETURNING last_generated_message_id;", chatId)
	if err := res.Scan(&messageId); err != nil {
		return 0, fmt.Errorf("error during generating message id: %w", err)
	}
	return messageId, nil
}

func (m *CommonProjection) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_common(id, title, created_timestamp, updated_timestamp) values ($1, $2, $3, $4)
		on conflict(id) do update set title = excluded.title, created_timestamp = excluded.created_timestamp, updated_timestamp = excluded.updated_timestamp
	`, event.ChatId, event.Title, event.AdditionalData.CreatedAt, nil)
	if err != nil {
		return err
	}
	LogWithTrace(ctx, m.slogLogger).Info(
		"Common chat created",
		"chat_id", event.ChatId,
		"title", event.Title,
	)

	return nil
}

func initializeMessageUnread(ctx context.Context, db *sql.DB, participantId, chatId int64) error {
	_, err := db.ExecContext(ctx, `
		with normalized_last_message as (
		    select coalesce(
				(select last_message_id from unread_messages_user_view where user_id = $1 and chat_id = $2),
		        0
		    ) as normalized_last_message_id
		)
		insert into unread_messages_user_view(user_id, chat_id, unread_messages, last_message_id)
		select 
		    $1, 
		    $2,
		    (SELECT count(m.id) FILTER(WHERE m.id > (SELECT normalized_last_message_id FROM normalized_last_message))
			FROM message m
			WHERE m.chat_id = $2),
		    (SELECT normalized_last_message_id FROM normalized_last_message)
		on conflict (user_id, chat_id) do update set unread_messages = excluded.unread_messages, last_message_id = excluded.last_message_id
	`, participantId, chatId)
	return err
}

func (m *CommonProjection) OnParticipantAdded(ctx context.Context, event *ParticipantAdded) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_participant(user_id, chat_id) values ($1, $2)
		on conflict(user_id, chat_id) do nothing
	`, event.ParticipantId, event.ChatId)
	if err != nil {
		return err
	}

	// because we select chat_common, inserted from this consumer group in ChatCreated handler
	_, err = m.db.ExecContext(ctx, `
		insert into chat_user_view(id, title, pinned, participant_id, created_timestamp, updated_timestamp) 
			select id, title, false, $2, created_timestamp, updated_timestamp from chat_common where id = $1
		on conflict(participant_id, id) do update set title = excluded.title, pinned = excluded.pinned, created_timestamp = excluded.created_timestamp, updated_timestamp = excluded.updated_timestamp
	`, event.ChatId, event.ParticipantId)
	if err != nil {
		return err
	}

	// recalc in case an user was added after
	// and it will fix the situation when there wasn't an event "increase unreads" because of lack an record in chat_participant
	err = initializeMessageUnread(ctx, m.db, event.ParticipantId, event.ChatId)
	if err != nil {
		return err
	}

	LogWithTrace(ctx, m.slogLogger).Info(
		"Handling participant added for per user chat view",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
	)

	LogWithTrace(ctx, m.slogLogger).Info(
		"Participant added into common chat",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnChatPinned(ctx context.Context, event *ChatPinned) error {
	_, err := m.db.ExecContext(ctx, `
		update chat_user_view
		set pinned = $3
		where id = $1 and participant_id = $2
	`, event.ChatId, event.ParticipantId, event.Pinned)
	if err != nil {
		return err
	}
	LogWithTrace(ctx, m.slogLogger).Info(
		"Chat pinned",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
		"pinned", event.Pinned,
	)
	return nil
}

func (m *CommonProjection) OnMessageCreated(ctx context.Context, event *MessageCreated) error {
	_, err := m.db.ExecContext(ctx, `
		insert into message(id, chat_id, owner_id, content, created_timestamp, updated_timestamp) 
			values ($1, $2, $3, $4, $5, $6)
		on conflict(chat_id, id) do update set owner_id = excluded.owner_id, content = excluded.content, created_timestamp = excluded.created_timestamp, updated_timestamp = excluded.updated_timestamp
	`, event.Id, event.ChatId, event.OwnerId, event.Content, event.AdditionalData.CreatedAt, nil)
	if err != nil {
		return err
	}
	LogWithTrace(ctx, m.slogLogger).Info(
		"Handling message added",
		"id", event.Id,
		"user_id", event.OwnerId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnUnreadMessageIncreased(ctx context.Context, event *UnreadMessageIncreased) error {
	r, err := m.db.ExecContext(ctx, `
		UPDATE unread_messages_user_view 
		SET unread_messages = unread_messages + 1
		WHERE user_id = $1 AND chat_id = $2;
	`, event.ParticipantId, event.ChatId)
	if err != nil {
		return fmt.Errorf("error during increasing unread messages: %w", err)
	}

	affected, err := r.RowsAffected()
	if err != nil {
		return fmt.Errorf("error during increasing unread messages: %w", err)
	}

	if affected == 0 {
		err = initializeMessageUnread(ctx, m.db, event.ParticipantId, event.ChatId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *CommonProjection) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded) error {
	// actually it should be an update
	// but we give a chance to create a row unread_messages_user_view in case lack of it
	// so message read event has a self-healing effect
	_, err := m.db.ExecContext(ctx, `
		with normalized_given_message as (
		    select coalesce(
		    	(select id from message where chat_id = $2 and id = $3),
				(select max(id) from message where chat_id = $2)
		    ) as normalized_message_id
		)
		insert into unread_messages_user_view(user_id, chat_id, unread_messages, last_message_id)
		select 
		    $1, 
		    $2,
		    (SELECT count(m.id) FILTER(WHERE m.id > (select normalized_message_id from normalized_given_message))
			FROM message m
			WHERE m.chat_id = $2),
		    (select normalized_message_id from normalized_given_message)
		on conflict (user_id, chat_id) do update set unread_messages = excluded.unread_messages, last_message_id = excluded.last_message_id
	`, event.ParticipantId, event.ChatId, event.MessageId)
	if err != nil {
		return fmt.Errorf("error during read messages: %w", err)
	}
	return nil
}

func (m *CommonProjection) GetParticipants(ctx context.Context, chatId int64) ([]int64, error) {
	res := []int64{}
	rows, err := m.db.QueryContext(ctx, "select user_id from chat_participant where chat_id = $1;", chatId)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid int64
		err = rows.Scan(&pid)
		if err != nil {
			return res, err
		}
		res = append(res, pid)
	}
	return res, nil
}

type MessageViewDto struct {
	Id      int64  `json:"id"`
	OwnerId int64  `json:"ownerId"`
	Content string `json:"content"`
}

func (m *CommonProjection) GetMessages(ctx context.Context, chatId int64) ([]MessageViewDto, error) {
	ma := []MessageViewDto{}

	rows, err := m.db.QueryContext(ctx, `
		select id, owner_id, content 
		from message 
		where chat_id = $1 
		order by id desc
	`, chatId)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd MessageViewDto
		err = rows.Scan(&cd.Id, &cd.OwnerId, &cd.Content)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}

type ChatViewDto struct {
	Id             int64  `json:"id"`
	Title          string `json:"title"`
	Pinned         bool   `json:"pinned"`
	UnreadMessages int64  `json:"unreadMessages"`
}

func (m *CommonProjection) GetChats(ctx context.Context, participantId int64) ([]ChatViewDto, error) {
	ma := []ChatViewDto{}

	rows, err := m.db.QueryContext(ctx, `
		select ch.id, ch.title, ch.pinned, coalesce(m.unread_messages, 0)
		from chat_user_view ch
		left join unread_messages_user_view m on (ch.id = m.chat_id and m.user_id = $1)
		where ch.participant_id = $1
		order by (ch.pinned, ch.updated_timestamp) desc
	`, participantId)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd ChatViewDto
		err = rows.Scan(&cd.Id, &cd.Title, &cd.Pinned, &cd.UnreadMessages)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}
