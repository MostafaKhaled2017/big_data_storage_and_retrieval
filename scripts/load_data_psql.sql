\set ON_ERROR_STOP on

\if :{?target_db}
\else
\set target_db customer_campaign_analytics
\endif

\if :{?data_dir}
\else
\set data_dir ./data/processed
\endif

\echo Ensuring database :target_db exists...
SELECT format('CREATE DATABASE %I', :'target_db')
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = :'target_db'
)\gexec

\connect :target_db
\echo Connected to database :target_db

CREATE TABLE IF NOT EXISTS users (
    user_id varchar(50) PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS clients (
    client_id varchar(50) PRIMARY KEY NOT NULL,
    user_id varchar(50),
    first_purchase_date date,
    user_device_id varchar(50),
    CONSTRAINT fk_users_user_id_to_clients_user_id
        FOREIGN KEY (user_id) REFERENCES users (user_id)
);

CREATE TABLE IF NOT EXISTS products (
    product_id varchar(50) PRIMARY KEY NOT NULL,
    category_id varchar(50),
    category_code text,
    brand text
);

CREATE TABLE IF NOT EXISTS events (
    event_id varchar(50) PRIMARY KEY NOT NULL,
    event_time timestamp WITH TIME ZONE,
    event_type text,
    user_id varchar(50),
    product_id varchar(50),
    price numeric,
    user_session text,
    CONSTRAINT fk_users_user_id_to_events_user_id
        FOREIGN KEY (user_id) REFERENCES users (user_id),
    CONSTRAINT fk_products_product_id_to_events_product_id
        FOREIGN KEY (product_id) REFERENCES products (product_id)
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_pk varchar(50) PRIMARY KEY NOT NULL,
    campaign_id varchar,
    campaign_type text,
    channel text,
    topic text,
    started_at timestamp,
    finished_at timestamp,
    total_count integer,
    ab_test boolean,
    warmup_mode boolean,
    hour_limit numeric,
    subject_length numeric,
    subject_with_personalization boolean,
    subject_with_deadline boolean,
    subject_with_emoji boolean,
    subject_with_bonuses boolean,
    subject_with_discount boolean,
    subject_with_saleout boolean,
    is_test boolean,
    position integer,
    CONSTRAINT uq_campaign_id_type UNIQUE (campaign_id, campaign_type)
);

CREATE TABLE IF NOT EXISTS messages (
    row_id varchar(50) PRIMARY KEY NOT NULL,
    message_id varchar(50),
    campaign_id varchar(50),
    message_type text,
    client_id varchar(50),
    user_id varchar(50),
    channel text,
    category text,
    platform text,
    email_provider text,
    stream text,
    date date,
    sent_at timestamp,
    is_opened boolean,
    opened_first_time_at timestamp,
    opened_last_time_at timestamp,
    is_clicked boolean,
    clicked_first_time_at timestamp,
    clicked_last_time_at timestamp,
    is_unsubscribed boolean,
    unsubscribed_at timestamp,
    is_hard_bounced boolean,
    hard_bounced_at timestamp,
    is_soft_bounced boolean,
    soft_bounced_at timestamp,
    is_complained boolean,
    complained_at timestamp,
    is_blocked boolean,
    blocked_at timestamp,
    is_purchased boolean,
    purchased_at timestamp,
    created_at timestamp,
    updated_at timestamp,
    user_device_id varchar(50),
    CONSTRAINT fk_clients_client_id_to_messages_client_id
        FOREIGN KEY (client_id) REFERENCES clients (client_id),
    CONSTRAINT fk_users_user_id_to_messages_user_id
        FOREIGN KEY (user_id) REFERENCES users (user_id),
    CONSTRAINT campaign_messages
        FOREIGN KEY (campaign_id, message_type)
        REFERENCES campaigns (campaign_id, campaign_type)
);

CREATE TABLE IF NOT EXISTS friends (
    user_id varchar(50) NOT NULL,
    friend_id varchar(50) NOT NULL,
    CONSTRAINT composite_pk PRIMARY KEY (user_id, friend_id),
    CONSTRAINT fk_users_user_id_to_friends_user_id
        FOREIGN KEY (user_id) REFERENCES users (user_id),
    CONSTRAINT fk_users_user_id_to_friends_friend_id
        FOREIGN KEY (friend_id) REFERENCES users (user_id)
);

CREATE SEQUENCE IF NOT EXISTS events_event_id_seq;

DROP TABLE IF EXISTS stg_campaigns;
DROP TABLE IF EXISTS stg_clients;
DROP TABLE IF EXISTS stg_events;
DROP TABLE IF EXISTS stg_messages;
DROP TABLE IF EXISTS stg_friends;

CREATE UNLOGGED TABLE stg_campaigns (
    id text,
    campaign_type text,
    channel text,
    topic text,
    started_at text,
    finished_at text,
    total_count text,
    ab_test text,
    warmup_mode text,
    hour_limit text,
    subject_length text,
    subject_with_personalization text,
    subject_with_deadline text,
    subject_with_emoji text,
    subject_with_bonuses text,
    subject_with_discount text,
    subject_with_saleout text,
    is_test text,
    position text
);

CREATE UNLOGGED TABLE stg_clients (
    client_id text,
    first_purchase_date text,
    user_id text,
    user_device_id text
);

CREATE UNLOGGED TABLE stg_events (
    event_time text,
    event_type text,
    product_id text,
    category_id text,
    category_code text,
    brand text,
    price text,
    user_id text,
    user_session text
);

CREATE UNLOGGED TABLE stg_messages (
    id text,
    message_id text,
    campaign_id text,
    message_type text,
    client_id text,
    channel text,
    category text,
    platform text,
    email_provider text,
    stream text,
    date text,
    sent_at text,
    is_opened text,
    opened_first_time_at text,
    opened_last_time_at text,
    is_clicked text,
    clicked_first_time_at text,
    clicked_last_time_at text,
    is_unsubscribed text,
    unsubscribed_at text,
    is_hard_bounced text,
    hard_bounced_at text,
    is_soft_bounced text,
    soft_bounced_at text,
    is_complained text,
    complained_at text,
    is_blocked text,
    blocked_at text,
    is_purchased text,
    purchased_at text,
    created_at text,
    updated_at text,
    user_device_id text,
    user_id text
);

CREATE UNLOGGED TABLE stg_friends (
    friend1 text,
    friend2 text
);

\echo Loading staging CSV files...
\copy stg_campaigns FROM '__CAMPAIGNS_FILE__' WITH (FORMAT csv, HEADER true)
\copy stg_clients FROM '__CLIENTS_FILE__' WITH (FORMAT csv, HEADER true)
\copy stg_events FROM '__EVENTS_FILE__' WITH (FORMAT csv, HEADER true)
\copy stg_messages FROM '__MESSAGES_FILE__' WITH (FORMAT csv, HEADER true)
\copy stg_friends FROM '__FRIENDS_FILE__' WITH (FORMAT csv, HEADER true)

\echo Refreshing target tables...
TRUNCATE TABLE
    friends,
    messages,
    events,
    products,
    clients,
    campaigns,
    users
RESTART IDENTITY CASCADE;

ALTER SEQUENCE events_event_id_seq RESTART WITH 1;

INSERT INTO users (user_id)
SELECT DISTINCT user_id
FROM (
    SELECT NULLIF(TRIM(user_id), '') AS user_id FROM stg_clients
    UNION ALL
    SELECT NULLIF(TRIM(user_id), '') AS user_id FROM stg_events
    UNION ALL
    SELECT NULLIF(TRIM(user_id), '') AS user_id FROM stg_messages
    UNION ALL
    SELECT NULLIF(TRIM(friend1), '') AS user_id FROM stg_friends
    UNION ALL
    SELECT NULLIF(TRIM(friend2), '') AS user_id FROM stg_friends
) u
WHERE user_id IS NOT NULL;

WITH ranked_clients AS (
    SELECT
        NULLIF(TRIM(client_id), '') AS client_id,
        NULLIF(TRIM(user_id), '') AS user_id,
        NULLIF(TRIM(first_purchase_date), '')::date AS first_purchase_date,
        NULLIF(TRIM(user_device_id), '') AS user_device_id,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(client_id), '')
            ORDER BY
                NULLIF(TRIM(first_purchase_date), '')::date ASC NULLS LAST,
                NULLIF(TRIM(user_id), '') ASC NULLS LAST,
                NULLIF(TRIM(user_device_id), '') ASC NULLS LAST
        ) AS rn
    FROM stg_clients
    WHERE NULLIF(TRIM(client_id), '') IS NOT NULL
)
INSERT INTO clients (client_id, user_id, first_purchase_date, user_device_id)
SELECT
    client_id,
    user_id,
    first_purchase_date,
    user_device_id
FROM ranked_clients
WHERE rn = 1;

WITH ranked_message_clients AS (
    SELECT
        NULLIF(TRIM(client_id), '') AS client_id,
        NULLIF(TRIM(user_id), '') AS user_id,
        NULLIF(TRIM(user_device_id), '') AS user_device_id,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(client_id), '')
            ORDER BY
                NULLIF(TRIM(user_id), '') ASC NULLS LAST,
                NULLIF(TRIM(user_device_id), '') ASC NULLS LAST
        ) AS rn
    FROM stg_messages
    WHERE NULLIF(TRIM(client_id), '') IS NOT NULL
)
INSERT INTO clients (client_id, user_id, first_purchase_date, user_device_id)
SELECT
    rmc.client_id,
    rmc.user_id,
    NULL::date AS first_purchase_date,
    rmc.user_device_id
FROM ranked_message_clients rmc
LEFT JOIN clients c
    ON c.client_id = rmc.client_id
WHERE rmc.rn = 1
  AND c.client_id IS NULL;

INSERT INTO products (product_id, category_id, category_code, brand)
SELECT DISTINCT ON (product_id)
    NULLIF(TRIM(product_id), '') AS product_id,
    NULLIF(TRIM(category_id), '') AS category_id,
    NULLIF(TRIM(category_code), '') AS category_code,
    NULLIF(TRIM(brand), '') AS brand
FROM stg_events
WHERE NULLIF(TRIM(product_id), '') IS NOT NULL
ORDER BY product_id, category_id NULLS LAST, category_code NULLS LAST, brand NULLS LAST;

INSERT INTO campaigns (
    campaign_pk,
    campaign_id,
    campaign_type,
    channel,
    topic,
    started_at,
    finished_at,
    total_count,
    ab_test,
    warmup_mode,
    hour_limit,
    subject_length,
    subject_with_personalization,
    subject_with_deadline,
    subject_with_emoji,
    subject_with_bonuses,
    subject_with_discount,
    subject_with_saleout,
    is_test,
    position
)
SELECT DISTINCT ON (campaign_id, campaign_type)
    LEFT(CONCAT(NULLIF(TRIM(id), ''), '|', NULLIF(TRIM(campaign_type), '')), 50) AS campaign_pk,
    NULLIF(TRIM(id), '') AS campaign_id,
    NULLIF(TRIM(campaign_type), '') AS campaign_type,
    NULLIF(TRIM(channel), '') AS channel,
    NULLIF(TRIM(topic), '') AS topic,
    NULLIF(TRIM(started_at), '')::timestamp AS started_at,
    NULLIF(TRIM(finished_at), '')::timestamp AS finished_at,
    NULLIF(TRIM(total_count), '')::integer AS total_count,
    CASE WHEN LOWER(NULLIF(TRIM(ab_test), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(ab_test), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS ab_test,
    CASE WHEN LOWER(NULLIF(TRIM(warmup_mode), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(warmup_mode), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS warmup_mode,
    NULLIF(TRIM(hour_limit), '')::numeric AS hour_limit,
    NULLIF(TRIM(subject_length), '')::numeric AS subject_length,
    CASE WHEN LOWER(NULLIF(TRIM(subject_with_personalization), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(subject_with_personalization), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS subject_with_personalization,
    CASE WHEN LOWER(NULLIF(TRIM(subject_with_deadline), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(subject_with_deadline), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS subject_with_deadline,
    CASE WHEN LOWER(NULLIF(TRIM(subject_with_emoji), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(subject_with_emoji), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS subject_with_emoji,
    CASE WHEN LOWER(NULLIF(TRIM(subject_with_bonuses), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(subject_with_bonuses), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS subject_with_bonuses,
    CASE WHEN LOWER(NULLIF(TRIM(subject_with_discount), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(subject_with_discount), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS subject_with_discount,
    CASE WHEN LOWER(NULLIF(TRIM(subject_with_saleout), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(subject_with_saleout), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS subject_with_saleout,
    CASE WHEN LOWER(NULLIF(TRIM(is_test), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_test), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_test,
    NULLIF(TRIM(position), '')::integer AS position
FROM stg_campaigns
WHERE NULLIF(TRIM(id), '') IS NOT NULL
  AND NULLIF(TRIM(campaign_type), '') IS NOT NULL
ORDER BY campaign_id, campaign_type, finished_at DESC NULLS LAST, started_at DESC NULLS LAST;

INSERT INTO events (event_id, event_time, event_type, user_id, product_id, price, user_session)
SELECT
    CONCAT('ev_', nextval('events_event_id_seq')) AS event_id,
    NULLIF(TRIM(event_time), '')::timestamp AT TIME ZONE 'UTC' AS event_time,
    NULLIF(TRIM(event_type), '') AS event_type,
    NULLIF(TRIM(user_id), '') AS user_id,
    NULLIF(TRIM(product_id), '') AS product_id,
    NULLIF(TRIM(price), '')::numeric AS price,
    NULLIF(TRIM(user_session), '') AS user_session
FROM stg_events
WHERE NULLIF(TRIM(user_id), '') IS NOT NULL
  AND NULLIF(TRIM(product_id), '') IS NOT NULL;

INSERT INTO messages (
    row_id,
    message_id,
    campaign_id,
    message_type,
    client_id,
    user_id,
    channel,
    category,
    platform,
    email_provider,
    stream,
    date,
    sent_at,
    is_opened,
    opened_first_time_at,
    opened_last_time_at,
    is_clicked,
    clicked_first_time_at,
    clicked_last_time_at,
    is_unsubscribed,
    unsubscribed_at,
    is_hard_bounced,
    hard_bounced_at,
    is_soft_bounced,
    soft_bounced_at,
    is_complained,
    complained_at,
    is_blocked,
    blocked_at,
    is_purchased,
    purchased_at,
    created_at,
    updated_at,
    user_device_id
)
SELECT
    NULLIF(TRIM(id), '') AS row_id,
    NULLIF(TRIM(message_id), '') AS message_id,
    NULLIF(TRIM(campaign_id), '') AS campaign_id,
    NULLIF(TRIM(message_type), '') AS message_type,
    NULLIF(TRIM(client_id), '') AS client_id,
    NULLIF(TRIM(user_id), '') AS user_id,
    NULLIF(TRIM(channel), '') AS channel,
    NULLIF(TRIM(category), '') AS category,
    NULLIF(TRIM(platform), '') AS platform,
    NULLIF(TRIM(email_provider), '') AS email_provider,
    NULLIF(TRIM(stream), '') AS stream,
    NULLIF(TRIM(date), '')::date AS date,
    NULLIF(TRIM(sent_at), '')::timestamp AS sent_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_opened), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_opened), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_opened,
    NULLIF(TRIM(opened_first_time_at), '')::timestamp AS opened_first_time_at,
    NULLIF(TRIM(opened_last_time_at), '')::timestamp AS opened_last_time_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_clicked), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_clicked), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_clicked,
    NULLIF(TRIM(clicked_first_time_at), '')::timestamp AS clicked_first_time_at,
    NULLIF(TRIM(clicked_last_time_at), '')::timestamp AS clicked_last_time_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_unsubscribed), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_unsubscribed), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_unsubscribed,
    NULLIF(TRIM(unsubscribed_at), '')::timestamp AS unsubscribed_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_hard_bounced), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_hard_bounced), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_hard_bounced,
    NULLIF(TRIM(hard_bounced_at), '')::timestamp AS hard_bounced_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_soft_bounced), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_soft_bounced), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_soft_bounced,
    NULLIF(TRIM(soft_bounced_at), '')::timestamp AS soft_bounced_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_complained), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_complained), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_complained,
    NULLIF(TRIM(complained_at), '')::timestamp AS complained_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_blocked), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_blocked), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_blocked,
    NULLIF(TRIM(blocked_at), '')::timestamp AS blocked_at,
    CASE WHEN LOWER(NULLIF(TRIM(is_purchased), '')) IN ('1', 't', 'true', 'yes', 'y') THEN TRUE
         WHEN LOWER(NULLIF(TRIM(is_purchased), '')) IN ('0', 'f', 'false', 'no', 'n') THEN FALSE
         ELSE NULL END AS is_purchased,
    NULLIF(TRIM(purchased_at), '')::timestamp AS purchased_at,
    NULLIF(TRIM(created_at), '')::timestamp AS created_at,
    NULLIF(TRIM(updated_at), '')::timestamp AS updated_at,
    NULLIF(TRIM(user_device_id), '') AS user_device_id
FROM stg_messages
WHERE NULLIF(TRIM(id), '') IS NOT NULL
  AND NULLIF(TRIM(client_id), '') IS NOT NULL
  AND NULLIF(TRIM(user_id), '') IS NOT NULL
  AND NULLIF(TRIM(campaign_id), '') IS NOT NULL
  AND NULLIF(TRIM(message_type), '') IS NOT NULL;

INSERT INTO friends (user_id, friend_id)
SELECT DISTINCT
    NULLIF(TRIM(friend1), '') AS user_id,
    NULLIF(TRIM(friend2), '') AS friend_id
FROM stg_friends
WHERE NULLIF(TRIM(friend1), '') IS NOT NULL
  AND NULLIF(TRIM(friend2), '') IS NOT NULL
  AND NULLIF(TRIM(friend1), '') <> NULLIF(TRIM(friend2), '');

DROP TABLE IF EXISTS stg_campaigns;
DROP TABLE IF EXISTS stg_clients;
DROP TABLE IF EXISTS stg_events;
DROP TABLE IF EXISTS stg_messages;
DROP TABLE IF EXISTS stg_friends;

\echo Data loading completed successfully.
