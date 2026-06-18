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
CREATE TABLE total_messages AS
SELECT
    'all' AS grp,
    COUNT(*) AS total_messages
FROM messages_stream
GROUP BY 'all'
;

-- create table unique_users
CREATE TABLE unique_кусшзшутеы AS
SELECT
    'all' AS grp,
    COUNT_DISTINCT(recipient_id) AS unique_recipients
FROM messages_stream
GROUP BY 'all'
;

-- create table user_statistics
CREATE TABLE user_statistics AS
SELECT
    user_id,
    COUNT(*) AS qty_messages,
    COUNT_DISTINCT(recipient_id) AS unique_recipients
FROM messages_stream
GROUP BY user_id
;