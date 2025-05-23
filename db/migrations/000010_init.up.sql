create sequence chat_id_sequence;

-- partition by chat_id
create table chat_common(
    id bigint primary key,
    title varchar(512) not null,
    last_generated_message_id bigint not null default 0,
    created_timestamp timestamp not null
);

-- partition by chat_id
create table chat_participant(
    user_id bigint not null,
    chat_id bigint not null,
    primary key(user_id, chat_id)
);

-- partition by chat_id
create table message(
    id bigint not null,
    chat_id bigint not null,
    owner_id bigint not null,
    content text not null,
    created_timestamp timestamp not null,
    updated_timestamp timestamp,
    primary key (chat_id, id)
);

-- partition by user_id
create table chat_user_view(
    id bigint not null,
    title varchar(512) not null,
    pinned boolean not null default false,
    user_id bigint not null,
    updated_timestamp timestamp not null,
    last_message_id bigint,
    last_message_content text,
    last_message_owner_id bigint,
    primary key (user_id, id)
);

create index chat_user_idx on chat_user_view(user_id, pinned, updated_timestamp, id);

-- partition by user_id
create table unread_messages_user_view(
    user_id bigint not null,
    chat_id bigint not null,
    unread_messages bigint not null default 0,
    last_message_id bigint not null default 0,
    primary key (user_id, chat_id)
);

create table technical(
    id int primary key,
    need_to_fast_forward_sequences bool not null default false
);

