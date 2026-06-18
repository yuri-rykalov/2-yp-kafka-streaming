-- create stream
CREATE STREAM messages_stream (
    user_id INT, 
    recipient_id INT,
    `timestamp` BIGINT,
    `message` VARCHAR
) WITH (
    KAFKA_TOPIC='messages',
    VALUE_FORMAT='JSON'
)
;

-- read from messages_stream to ensure that data is coming
SELECT * FROM messages_stream EMIT CHANGES ;

-- create table total_users
CREATE TABLE total_users AS
WITH (
    KAFKA_TOPIC = 'table-input-topic',
    VALUE_FORMAT = 'JSON',
    KEY = 'user_id'
);

-- create table unique_users
CREATE TABLE unique_users AS
SELECT
    'all' AS grp,
    COUNT_DISTINCT(user_id) AS unique_users
FROM messages_stream
GROUP BY 'all'
EMIT CHANGES;

-- create table user_statistics
CREATE TABLE user_statistics AS
SELECT
    user_id,
    COUNT_DISTINCT(recipient_id) AS unique_recipients
FROM messages_stream
GROUP BY user_id
EMIT CHANGES;