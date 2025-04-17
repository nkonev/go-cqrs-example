create sequence chat_id_sequence;

create table chat_common(
    id bigint primary key,
    title varchar(512) not null,
    created_timestamp timestamp not null,
    updated_timestamp timestamp
);

create table chat_participant(
    user_id bigint not null,
    chat_id bigint not null,
    primary key(user_id, chat_id)
);

create table chat_user_view(
    id bigint not null,
    title varchar(512) not null,
    pinned boolean not null default false,
    participant_id bigint not null,
    created_timestamp timestamp not null,
    updated_timestamp timestamp,
    primary key (participant_id, id)
);
