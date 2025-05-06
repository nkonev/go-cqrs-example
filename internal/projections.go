package internal

import (
	"context"
	"fmt"
	"log/slog"
)

type CommonProjection struct {
	db         *DB
	slogLogger *slog.Logger
}

func NewCommonProjection(db *DB, slogLogger *slog.Logger) *CommonProjection {
	return &CommonProjection{
		db:         db,
		slogLogger: slogLogger,
	}
}

func (m *CommonProjection) GetNextChatId(ctx context.Context, tx *Tx) (int64, error) {
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

func (m *CommonProjection) InitializeChatIdSequenceIfNeed(ctx context.Context, tx *Tx) error {
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

func (m *CommonProjection) GetNextMessageId(ctx context.Context, tx *Tx, chatId int64) (int64, error) {
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

func (m *CommonProjection) GetChatIds(ctx context.Context, tx *Tx) ([]int64, error) {
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

func (m *CommonProjection) InitializeMessageIdSequenceIfNeed(ctx context.Context, tx *Tx, chatId int64) error {
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
		r := tx.QueryRowContext(ctx, "SELECT coalesce(max(id), 0) from message where chat_id = $1", chatId)
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

func (m *CommonProjection) UnsetIsNeedToFastForwardSequences(ctx context.Context, tx *Tx) error {
	_, err := tx.ExecContext(ctx, "delete from technical where need_to_fast_forward_sequences = true")
	return err
}

func (m *CommonProjection) GetIsNeedToFastForwardSequences(ctx context.Context, tx *Tx) (bool, error) {
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

func (m *CommonProjection) SetXactFastForwardSequenceLock(ctx context.Context, tx *Tx) error {
	_, err := tx.ExecContext(ctx, "select pg_advisory_xact_lock($1, $2)", lockIdKey1, lockIdKey2)
	return err
}

func (m *CommonProjection) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_common(id, title, created_timestamp, updated_timestamp) values ($1, $2, $3, $4)
		on conflict(id) do update set title = excluded.title, created_timestamp = excluded.created_timestamp, updated_timestamp = excluded.updated_timestamp
	`, event.ChatId, event.Title, event.AdditionalData.CreatedAt, event.AdditionalData.CreatedAt)
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

func initializeMessageUnread(ctx context.Context, tx *Tx, participantId, chatId int64) error {
	_, err := tx.ExecContext(ctx, `
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
	if err != nil {
		return err
	}

	return nil
}

func (m *CommonProjection) OnParticipantAdded(ctx context.Context, event *ParticipantAdded) error {
	errOuter := Transact(ctx, m.db, func(tx *Tx) error {
		_, err := tx.ExecContext(ctx, `
		insert into chat_participant(user_id, chat_id) values ($1, $2)
		on conflict(user_id, chat_id) do nothing
	`, event.ParticipantId, event.ChatId)
		if err != nil {
			return err
		}

		// because we select chat_common, inserted from this consumer group in ChatCreated handler
		_, err = tx.ExecContext(ctx, `
		insert into chat_user_view(id, pinned, user_id, updated_timestamp) 
			select id, false, $2, updated_timestamp from chat_common where id = $1
		on conflict(user_id, id) do update set pinned = excluded.pinned, updated_timestamp = excluded.updated_timestamp
	`, event.ChatId, event.ParticipantId)
		if err != nil {
			return err
		}

		// recalc in case an user was added after
		return initializeMessageUnread(ctx, tx, event.ParticipantId, event.ChatId)
	})
	if errOuter != nil {
		return errOuter
	}

	LogWithTrace(ctx, m.slogLogger).Info(
		"Participant added into common chat",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnParticipantRemoved(ctx context.Context, event *ParticipantRemoved) error {
	errOuter := Transact(ctx, m.db, func(tx *Tx) error {
		_, err := tx.ExecContext(ctx, `
		delete from chat_participant where (user_id, chat_id) = ($1, $2)
	`, event.ParticipantId, event.ChatId)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
		delete from chat_user_view where (id, user_id) = ($1, $2)
	`, event.ChatId, event.ParticipantId)
		if err != nil {
			return err
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	LogWithTrace(ctx, m.slogLogger).Info(
		"Participant removed from common chat",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnMessageRemoved(ctx context.Context, event *MessageRemoved) error {
	errOuter := Transact(ctx, m.db, func(tx *Tx) error {
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

	LogWithTrace(ctx, m.slogLogger).Info(
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
	errOuter := Transact(ctx, m.db, func(tx *Tx) error {
		if !event.IsMessageOwner {
			r, err := tx.ExecContext(ctx, `
				UPDATE unread_messages_user_view 
				SET unread_messages = unread_messages + $3
				WHERE (user_id, chat_id) = ($1, $2);
			`, event.ParticipantId, event.ChatId, event.IncreaseOn)
			if err != nil {
				return fmt.Errorf("error during increasing unread messages: %w", err)
			}

			affected, err := r.RowsAffected()
			if err != nil {
				return fmt.Errorf("error during increasing unread messages: %w", err)
			}

			if affected == 0 {
				err = initializeMessageUnread(ctx, tx, event.ParticipantId, event.ChatId)
				if err != nil {
					return err
				}
			}
		} else {
			_, err := tx.ExecContext(ctx, `
				UPDATE unread_messages_user_view 
				SET last_message_id = (select max(id) from message where chat_id = $2)
				WHERE (user_id, chat_id) = ($1, $2);
			`, event.ParticipantId, event.ChatId)
			if err != nil {
				return fmt.Errorf("error during increasing unread messages: %w", err)
			}
		}

		_, err := tx.ExecContext(ctx, `
			update chat_user_view set updated_timestamp = $3 where (user_id, id) = ($1, $2)
		`, event.ParticipantId, event.ChatId, event.AdditionalData.CreatedAt)
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

func (m *CommonProjection) setUnreadMessages(ctx context.Context, participantId, chatId, messageId int64, noMessageIdSet bool) error {
	_, err := m.db.ExecContext(ctx, `
		with existing_message_id as (
			select (
				case 
					when $4 = true then 0
					else (
						select coalesce(
							(select id from message where chat_id = $2 and id = $3),
							(select max(id) as max from message where chat_id = $2),
							0
						)
					)
				end
			) as normalized_message_id
		),
		last_set_unread_message_id as (
			 select coalesce((select last_message_id from unread_messages_user_view where user_id = $1 and chat_id = $2), 0) as last_message_id
		),
		normalized_given_message as (
			select 
				case 
					when ((select normalized_message_id from existing_message_id) >= (select last_message_id from last_set_unread_message_id)) 
						THEN (select normalized_message_id from existing_message_id)
					else (select last_message_id from last_set_unread_message_id)
				end 
				as normalized_message_id
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
	`, participantId, chatId, messageId, noMessageIdSet)
	return err
}

func (m *CommonProjection) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded) error {
	// actually it should be an update
	// but we give a chance to create a row unread_messages_user_view in case lack of it
	// so message read event has a self-healing effect
	err := m.setUnreadMessages(ctx, event.ParticipantId, event.ChatId, event.MessageId, false)
	if err != nil {
		return fmt.Errorf("error during read messages: %w", err)
	}

	return nil
}

func (m *CommonProjection) OnUnreadMessageRefreshed(ctx context.Context, event *UnreadMessageRefreshed) error {
	err := m.setUnreadMessages(ctx, event.ParticipantId, event.ChatId, 0, true)
	if err != nil {
		return fmt.Errorf("error during refresh unread messages: %w", err)
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

	// it is optimized (all order by in the same table)
	// so querying a page (using keyset) from a large amount of chats is fast
	// it's the root cause why we use cqrs
	rows, err := m.db.QueryContext(ctx, `
		select ch.id, c.title, ch.pinned, coalesce(m.unread_messages, 0)
		from chat_user_view ch
		join chat_common c on ch.id = c.id
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
		err = rows.Scan(&cd.Id, &cd.Title, &cd.Pinned, &cd.UnreadMessages)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}
