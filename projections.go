package main

import (
	"context"
	"database/sql"
	"log/slog"
)

type CommonProjection struct {
	db         *sql.DB
	slogLogger *slog.Logger
}

func NewSubscriberProjection(db *sql.DB, slogLogger *slog.Logger) *CommonProjection {
	return &CommonProjection{
		db:         db,
		slogLogger: slogLogger,
	}
}

type UserChatProjection struct {
	db         *sql.DB
	slogLogger *slog.Logger
}

func NewUserChatProjection(db *sql.DB, slogLogger *slog.Logger) *UserChatProjection {
	return &UserChatProjection{
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

func (m *CommonProjection) OnParticipantAdded(ctx context.Context, event *ParticipantAdded) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_participant(user_id, chat_id) values ($1, $2)
		on conflict(user_id, chat_id) do nothing
	`, event.ParticipantId, event.ChatId)
	if err != nil {
		return err
	}
	LogWithTrace(ctx, m.slogLogger).Info(
		"Participant added into common chat",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnChatPinned(ctx context.Context, event *ChatPinned) error {
	return nil
}

func (m *UserChatProjection) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	return nil
}

func (m *UserChatProjection) OnParticipantAdded(ctx context.Context, event *ParticipantAdded) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_user_view(id, title, pinned, participant_id, created_timestamp, updated_timestamp) 
			select id, title, false, $2, created_timestamp, updated_timestamp from chat_common where id = $1
		on conflict(participant_id, id) do update set title = excluded.title, pinned = excluded.pinned, created_timestamp = excluded.created_timestamp, updated_timestamp = excluded.updated_timestamp
	`, event.ChatId, event.ParticipantId)
	if err != nil {
		return err
	}
	LogWithTrace(ctx, m.slogLogger).Info(
		"Handling participant added for per user chat view",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *UserChatProjection) OnChatPinned(ctx context.Context, event *ChatPinned) error {
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

type ChatViewDto struct {
	Id     int64  `json:"id"`
	Title  string `json:"title"`
	Pinned bool   `json:"pinned"`
}

func (m *UserChatProjection) GetChats(ctx context.Context, participantId int64) ([]ChatViewDto, error) {
	ma := []ChatViewDto{}

	rows, err := m.db.QueryContext(ctx, `
		select id, title, pinned 
		from chat_user_view 
		where participant_id = $1 
		order by (pinned, updated_timestamp) desc
	`, participantId)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd ChatViewDto
		err = rows.Scan(&cd.Id, &cd.Title, &cd.Pinned)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}
