create sequence chat_id_sequence;

-- partition by chat_id
create table chat_common(
    id bigint primary key,
    title varchar(512) not null,
    last_generated_message_id bigint not null default 0,
    created_timestamp timestamp not null,
    updated_timestamp timestamp
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
    created_timestamp timestamp not null,
    updated_timestamp timestamp,
    primary key (user_id, id)
);

-- partition by user_id
create table unread_messages_user_view(
    user_id bigint not null,
    chat_id bigint not null,
    unread_messages bigint not null default 0,
    last_message_id bigint not null default 0,
    primary key (user_id, chat_id)
);

-- partition by user_id
create table chat_user_view_revision(
    user_id bigint not null,
    partition_id int not null,
    offset_id bigint not null,
    primary key(user_id, partition_id)
);

-- partition by user_id
create table unread_messages_user_view_revision(
    user_id bigint not null,
    partition_id int not null,
    offset_id bigint not null,
    primary key(user_id, partition_id)
);
