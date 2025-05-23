package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"log/slog"
	"slices"
)

type CommonProjection struct {
	db         *db.DB
	slogLogger *slog.Logger
}

func NewCommonProjection(db *db.DB, slogLogger *slog.Logger) *CommonProjection {
	return &CommonProjection{
		db:         db,
		slogLogger: slogLogger,
	}
}

func (m *CommonProjection) GetNextChatId(ctx context.Context, tx *db.Tx) (int64, error) {
	r := tx.QueryRowContext(ctx, "select nextval('chat_id_sequence')")
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

func (m *CommonProjection) InitializeChatIdSequenceIfNeed(ctx context.Context, tx *db.Tx) error {
	r := tx.QueryRowContext(ctx, "SELECT is_called FROM chat_id_sequence")
	if r.Err() != nil {
		return r.Err()
	}
	var called bool
	err := r.Scan(&called)
	if err != nil {
		return err
	}

	if !called {
		r := tx.QueryRowContext(ctx, "SELECT coalesce(max(id), 0) from chat_common")
		if r.Err() != nil {
			return r.Err()
		}
		var maxChatId int64
		err := r.Scan(&maxChatId)
		if err != nil {
			return err
		}

		if maxChatId > 0 {
			m.slogLogger.Info("Fast-forwarding chatId sequence")
			_, err := tx.ExecContext(ctx, "SELECT setval('chat_id_sequence', $1, true)", maxChatId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *CommonProjection) GetNextMessageId(ctx context.Context, tx *db.Tx, chatId int64) (int64, error) {
	var messageId int64
	res := tx.QueryRowContext(ctx, "UPDATE chat_common SET last_generated_message_id = last_generated_message_id + 1 WHERE id = $1 RETURNING last_generated_message_id;", chatId)
	if res.Err() != nil {
		return 0, res.Err()
	}
	if err := res.Scan(&messageId); err != nil {
		return 0, fmt.Errorf("error during generating message id: %w", err)
	}
	return messageId, nil
}

func (m *CommonProjection) GetChatIds(ctx context.Context, tx *db.Tx) ([]int64, error) {
	ma := []int64{}
	rows, err := tx.QueryContext(ctx, `
		select c.id
		from chat_common c
		order by c.id asc 
	`)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var ii int64
		err = rows.Scan(&ii)
		if err != nil {
			return ma, err
		}
		ma = append(ma, ii)
	}
	return ma, nil
}

func (m *CommonProjection) InitializeMessageIdSequenceIfNeed(ctx context.Context, tx *db.Tx, chatId int64) error {
	r := tx.QueryRowContext(ctx, "SELECT coalesce(last_generated_message_id, 0) from chat_common where id = $1", chatId)
	if r.Err() != nil {
		return r.Err()
	}
	var currentGeneratedMessageId int64
	err := r.Scan(&currentGeneratedMessageId)
	if err != nil {
		return err
	}

	if currentGeneratedMessageId == 0 {
		r = tx.QueryRowContext(ctx, "SELECT coalesce(max(id), 0) from message where chat_id = $1", chatId)
		if r.Err() != nil {
			return r.Err()
		}
		var maxMessageId int64
		err := r.Scan(&maxMessageId)
		if err != nil {
			return err
		}

		if maxMessageId > 0 {
			m.slogLogger.Info("Fast-forwarding messageId sequence", "chat_id", chatId)

			_, err := tx.ExecContext(ctx, "update chat_common set last_generated_message_id = $2 where id = $1", chatId, maxMessageId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *CommonProjection) SetIsNeedToFastForwardSequences(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "insert into technical(id, need_to_fast_forward_sequences) values (1, true) on conflict (id) do update set need_to_fast_forward_sequences = excluded.need_to_fast_forward_sequences")
	return err
}

func (m *CommonProjection) UnsetIsNeedToFastForwardSequences(ctx context.Context, tx *db.Tx) error {
	_, err := tx.ExecContext(ctx, "delete from technical where need_to_fast_forward_sequences = true")
	return err
}

func (m *CommonProjection) GetIsNeedToFastForwardSequences(ctx context.Context, tx *db.Tx) (bool, error) {
	r := tx.QueryRowContext(ctx, "select exists(select * from technical where need_to_fast_forward_sequences = true)")
	var e bool
	err := r.Scan(&e)
	if err != nil {
		return false, err
	}
	return e, err
}

const lockIdKey1 = 1
const lockIdKey2 = 2

func (m *CommonProjection) SetXactFastForwardSequenceLock(ctx context.Context, tx *db.Tx) error {
	_, err := tx.ExecContext(ctx, "select pg_advisory_xact_lock($1, $2)", lockIdKey1, lockIdKey2)
	return err
}

func (m *CommonProjection) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_common(id, title, created_timestamp) values ($1, $2, $3)
		on conflict(id) do update set title = excluded.title, created_timestamp = excluded.created_timestamp
	`, event.ChatId, event.Title, event.AdditionalData.CreatedAt)
	if err != nil {
		return err
	}
	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Common chat created",
		"chat_id", event.ChatId,
		"title", event.Title,
	)

	return nil
}

func (m *CommonProjection) OnChatEdited(ctx context.Context, event *ChatEdited) error {
	_, err := m.db.ExecContext(ctx, `
		update chat_common
		set title = $2
		where id = $1
	`, event.ChatId, event.Title)
	if err != nil {
		return err
	}
	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Common chat edited",
		"chat_id", event.ChatId,
		"title", event.Title,
	)

	return nil
}

func (m *CommonProjection) OnChatRemoved(ctx context.Context, event *ChatDeleted) error {
	_, err := m.db.ExecContext(ctx, `
		delete from chat_common
		where id = $1
	`, event.ChatId)
	if err != nil {
		return err
	}
	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Common chat removed",
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) initializeMessageUnreadMultipleParticipants(ctx context.Context, tx *db.Tx, participantIds []int64, chatId int64) error {
	return m.setUnreadMessages(ctx, tx, participantIds, chatId, 0, true, false)
}

func (m *CommonProjection) OnParticipantAdded(ctx context.Context, event *ParticipantsAdded) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		r := tx.QueryRowContext(ctx, "select exists (select * from chat_common where id = $1)", event.ChatId)
		if r.Err() != nil {
			return r.Err()
		}
		var exists bool
		err := r.Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			m.slogLogger.Info("Skipping ParticipantsAdded because there is no chat", "chat_id", event.ChatId)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		with input_data as (
			select unnest(cast ($1 as bigint[])) as user_id, cast ($2 as bigint) as chat_id
		)
		insert into chat_participant(user_id, chat_id)
		select user_id, chat_id from input_data
		on conflict(user_id, chat_id) do nothing
	`, event.ParticipantIds, event.ChatId)
		if err != nil {
			return err
		}

		// because we select chat_common, inserted from this consumer group in ChatCreated handler
		_, err = tx.ExecContext(ctx, `
		with user_input as (
			select unnest(cast ($1 as bigint[])) as user_id
		),
		input_data as (
			select c.id as chat_id, c.title as title, false as pinned, u.user_id as user_id, cast ($3 as timestamp) as updated_timestamp
			from user_input u
			cross join (select cc.id, cc.title from chat_common cc where cc.id = $2) c 
		)
		insert into chat_user_view(id, title, pinned, user_id, updated_timestamp) 
			select chat_id, title, pinned, user_id, updated_timestamp from input_data
		on conflict(user_id, id) do update set pinned = excluded.pinned, title = excluded.title, updated_timestamp = excluded.updated_timestamp
	`, event.ParticipantIds, event.ChatId, event.AdditionalData.CreatedAt)
		if err != nil {
			return err
		}

		// recalc in case an user was added after
		err = m.initializeMessageUnreadMultipleParticipants(ctx, tx, event.ParticipantIds, event.ChatId)
		if err != nil {
			return err
		}

		err = m.setLastMessage(ctx, tx, event.ParticipantIds, event.ChatId)
		if err != nil {
			return err
		}
		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Participant added into common chat",
		"user_id", event.ParticipantIds,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnParticipantRemoved(ctx context.Context, event *ParticipantDeleted) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		_, err := tx.ExecContext(ctx, `
		delete from chat_participant where user_id = any($1) and chat_id = $2
	`, event.ParticipantIds, event.ChatId)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
		delete from chat_user_view where user_id = any($1) and id = $2
	`, event.ParticipantIds, event.ChatId)
		if err != nil {
			return err
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Participant removed from common chat",
		"user_id", event.ParticipantIds,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnMessageRemoved(ctx context.Context, event *MessageDeleted) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		_, err := tx.ExecContext(ctx, `
		delete from message where (id, chat_id) = ($1, $2)
	`, event.MessageId, event.ChatId)
		if err != nil {
			return err
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Message removed from common chat",
		"message_id", event.MessageId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnChatPinned(ctx context.Context, event *ChatPinned) error {
	_, err := m.db.ExecContext(ctx, `
		update chat_user_view
		set pinned = $3
		where (id, user_id) = ($1, $2)
	`, event.ChatId, event.ParticipantId, event.Pinned)
	if err != nil {
		return err
	}

	logger.LogWithTrace(ctx, m.slogLogger).Info(
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
	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Handling message added",
		"id", event.Id,
		"user_id", event.OwnerId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnMessageEdited(ctx context.Context, event *MessageEdited) error {
	_, err := m.db.ExecContext(ctx, `
		update message
		set	content = $3, updated_timestamp = $4
		where chat_id = $2 and id = $1 
	`, event.Id, event.ChatId, event.Content, event.AdditionalData.CreatedAt)
	if err != nil {
		return err
	}
	logger.LogWithTrace(ctx, m.slogLogger).Info(
		"Handling message edited",
		"id", event.Id,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) setLastMessage(ctx context.Context, tx *db.Tx, participantIds []int64, chatId int64) error {

	_, err := tx.ExecContext(ctx, `
				with last_message as (
					select 
						m.id,
						m.owner_id, 
						m.content 
					from message m 
					where m.chat_id = $2 and m.id = (select max(mm.id) from message mm where mm.chat_id = $2)
				)
				UPDATE chat_user_view 
				SET 
					last_message_id = (select id from last_message),
					last_message_content = (select content from last_message),
					last_message_owner_id = (select owner_id from last_message)
				WHERE user_id = any($1) and id = $2;
			`, participantIds, chatId)
	if err != nil {
		return fmt.Errorf("error during setting last message: %w", err)
	}
	return nil
}

func (m *CommonProjection) OnChatViewRefreshed(ctx context.Context, event *ChatViewRefreshed) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		if event.UnreadMessagesAction == UnreadMessagesActionIncrease {
			participantIdsWithoutOwner := utils.GetSliceWithout(event.OwnerId, event.ParticipantIds)
			var ownerId *int64
			if slices.Contains(event.ParticipantIds, event.OwnerId) { // for batches without owner
				ownerId = &event.OwnerId
			}

			// not owners
			if len(participantIdsWithoutOwner) > 0 {
				_, err := tx.ExecContext(ctx, `
					UPDATE unread_messages_user_view 
					SET unread_messages = unread_messages + $3
					WHERE user_id = any($1) and chat_id = $2;
				`, participantIdsWithoutOwner, event.ChatId, event.IncreaseOn)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}
			}

			// owner
			if ownerId != nil {
				_, err := tx.ExecContext(ctx, `
					UPDATE unread_messages_user_view 
					SET last_message_id = (select max(id) from message where chat_id = $2)
					WHERE (user_id, chat_id) = ($1, $2);
				`, *ownerId, event.ChatId)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}
			}
		} else if event.UnreadMessagesAction == UnreadMessagesActionRefresh {
			err := m.setUnreadMessages(ctx, tx, event.ParticipantIds, event.ChatId, 0, true, true)
			if err != nil {
				return err
			}
		}

		if event.LastMessageAction == LastMessageActionRefresh {
			err := m.setLastMessage(ctx, tx, event.ParticipantIds, event.ChatId)
			if err != nil {
				return err
			}
		}

		if event.ChatCommonAction == ChatCommonActionRefresh {
			_, err := tx.ExecContext(ctx, `
					UPDATE chat_user_view 
					SET title = $3
					WHERE user_id = any($1) and id = $2;
				`, event.ParticipantIds, event.ChatId, event.Title)
			if err != nil {
				return fmt.Errorf("error during increasing unread messages: %w", err)
			}
		}

		_, err := tx.ExecContext(ctx, `
				update chat_user_view set updated_timestamp = $3 where user_id = any($1) and id = $2
			`, event.ParticipantIds, event.ChatId, event.AdditionalData.CreatedAt)
		if err != nil {
			return err
		}

		return nil
	})

	if errOuter != nil {
		return errOuter
	}
	return nil
}

func (m *CommonProjection) setUnreadMessages(ctx context.Context, tx *db.Tx, participantIds []int64, chatId, messageId int64, needSet, needRefresh bool) error {
	_, err := tx.ExecContext(ctx, `
		with normalized_user as (
			select unnest(cast ($1 as bigint[])) as user_id
		),
		last_message as (
			select 
				coalesce(ww.last_message_id, 0) as last_message_id,
				nu.user_id
			from (
				select
					(case
						when exists(select * from unread_messages_user_view uw where uw.chat_id = $2 and uw.user_id = w.user_id and uw.last_message_id > 0)
						then coalesce(
							(select id as last_message_id from message m where m.chat_id = $2 and m.id = w.last_message_id),
							(select max(id) as max from message m where m.chat_id = $2 and $5 = true)
						)
					end) as last_message_id,
					w.user_id
				from unread_messages_user_view w 
				where w.chat_id = $2 and w.user_id = any($1)
			) ww
			right join normalized_user nu on ww.user_id = nu.user_id
		),
		existing_message as (
			select coalesce(
				(select id from message where chat_id = $2 and id = $3),
				(select max(id) as max from message where chat_id = $2),
				0
			) as normalized_message_id
		),
		normalized_given_message as (
			select 
				n.user_id,
				(case 
					when $4 = true then (select l.last_message_id from last_message l where l.user_id = n.user_id)
					else (select normalized_message_id from existing_message) 
				end) as normalized_message_id
			from normalized_user n
		),
		input_data as (
			select
				ngm.user_id as user_id,
				cast ($2 as bigint) as chat_id,
				(SELECT count(m.id) FILTER(WHERE m.id > (select normalized_message_id from normalized_given_message n where n.user_id = ngm.user_id))
					FROM message m
					WHERE m.chat_id = $2) as unread_messages,
				ngm.normalized_message_id as last_message_id
			from normalized_given_message ngm
		)
		insert into unread_messages_user_view(user_id, chat_id, unread_messages, last_message_id)
		select 
			idt.user_id,
			idt.chat_id,
			idt.unread_messages,
			idt.last_message_id
		from input_data idt
		on conflict (user_id, chat_id) do update set unread_messages = excluded.unread_messages, last_message_id = excluded.last_message_id
	`, participantIds, chatId, messageId, needSet, needRefresh)
	return err
}

func (m *CommonProjection) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded) error {
	// actually it should be an update
	// but we give a chance to create a row unread_messages_user_view in case lack of it
	// so message read event has a self-healing effect
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		return m.setUnreadMessages(ctx, tx, []int64{event.ParticipantId}, event.ChatId, event.MessageId, false, false)
	})
	if errOuter != nil {
		return fmt.Errorf("error during read messages: %w", errOuter)
	}

	return nil
}

func (m *CommonProjection) GetParticipants(ctx context.Context, chatId int64) ([]int64, error) {
	res := []int64{}
	rows, err := m.db.QueryContext(ctx, "select user_id from chat_participant where chat_id = $1 order by user_id", chatId)
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

func (m *CommonProjection) GetMessageOwner(ctx context.Context, chatId, messageId int64) (int64, error) {
	r := m.db.QueryRowContext(ctx, "select owner_id from message where (chat_id, id) = ($1, $2)", chatId, messageId)
	if r.Err() != nil {
		return 0, r.Err()
	}
	var ownerId int64
	err := r.Scan(&ownerId)
	if err != nil {
		return 0, err
	}
	return ownerId, nil
}

func (m *CommonProjection) GetLastMessageReaded(ctx context.Context, chatId, userId int64) (int64, bool, int64, error) {
	r := m.db.QueryRowContext(ctx, `
	select 
	    um.last_message_id, 
	    exists(select * from message m where m.chat_id = $2 and m.id = um.last_message_id),
	    (select max(id) from message m where m.chat_id = $2)
	from unread_messages_user_view um 
    where (um.user_id, um.chat_id) = ($1, $2)
	`, userId, chatId)
	if r.Err() != nil {
		return 0, false, 0, r.Err()
	}
	var lastReadedMessageId int64
	var has bool
	var maxMessageId int64
	err := r.Scan(&lastReadedMessageId, &has, &maxMessageId)
	if err != nil {
		return 0, false, 0, err
	}
	return lastReadedMessageId, has, maxMessageId, nil
}

func (m *CommonProjection) GetLastMessageId(ctx context.Context, chatId int64) (int64, error) {
	r := m.db.QueryRowContext(ctx, `
	select coalesce(inn.max_id, 0) 
	from (select max(id) as max_id from message m where m.chat_id = $1) inn
	`, chatId)
	if r.Err() != nil {
		return 0, r.Err()
	}
	var maxMessageId int64
	err := r.Scan(&maxMessageId)
	if err != nil {
		return 0, err
	}
	return maxMessageId, nil
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
	Id                 int64   `json:"id"`
	Title              string  `json:"title"`
	Pinned             bool    `json:"pinned"`
	UnreadMessages     int64   `json:"unreadMessages"`
	LastMessageId      *int64  `json:"lastMessageId"`
	LastMessageOwnerId *int64  `json:"lastMessageOwnerId"`
	LastMessageContent *string `json:"lastMessageContent"`
}

func (m *CommonProjection) GetChats(ctx context.Context, participantId int64) ([]ChatViewDto, error) {
	ma := []ChatViewDto{}

	// it is optimized (all order by in the same table)
	// so querying a page (using keyset) from a large amount of chats is fast
	// it's the root cause why we use cqrs
	rows, err := m.db.QueryContext(ctx, `
		select ch.id, ch.title, ch.pinned, coalesce(m.unread_messages, 0), ch.last_message_id, ch.last_message_owner_id, ch.last_message_content
		from chat_user_view ch
		join unread_messages_user_view m on (ch.id = m.chat_id and m.user_id = $1)
		where ch.user_id = $1
		order by (ch.pinned, ch.updated_timestamp, ch.id) desc 
	`, participantId)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd ChatViewDto
		err = rows.Scan(&cd.Id, &cd.Title, &cd.Pinned, &cd.UnreadMessages, &cd.LastMessageId, &cd.LastMessageOwnerId, &cd.LastMessageContent)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}

func (m *CommonProjection) GetChatByUserIdAndChatId(ctx context.Context, userId, chatId int64) (string, error) {
	r := m.db.QueryRowContext(ctx, "select c.title from chat_user_view ch join chat_common c on ch.id = c.id where ch.user_id = $1 and ch.id = $2", userId, chatId)
	if r.Err() != nil {
		return "", r.Err()
	}
	var t string
	err := r.Scan(&t)
	if err != nil {
		return "", err
	}
	return t, nil
}
