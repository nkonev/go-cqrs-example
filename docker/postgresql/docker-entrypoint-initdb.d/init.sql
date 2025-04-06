CREATE FUNCTION utc_now() returns TIMESTAMP AS $$ SELECT now() at time zone 'utc' $$ LANGUAGE SQL;

create table subscriber(
    subscriber_id uuid primary key,
    email varchar(256) not null,
    created_timestamp timestamp default utc_now()
);

create table activity_timeline(
    created_timestamp timestamp primary key,
    subscriber_id uuid not null,
    activity_type varchar(32) not null,
    details text not null
);
