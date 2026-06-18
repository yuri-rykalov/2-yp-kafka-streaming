import kafka_app.logger

from kafka_app.producer import KafkaProducer
from utils.user_message import UserMessages


# Запустим Producer и отправим сообщения
producer = KafkaProducer("localhost:9094")

# Запустим test run, чтобы проверить агрегацию ksqlDB
user_messages = UserMessages()

# (user_id, recipient_id)
# 3 unique user_id and 3 unique recipient_id
users_recipients = [
    (1001, 1002), 
    (1001, 1003), 
    (1001, 1004), 
    (1002, 1003), 
    (1002, 1004), 
    (1003, 1004), 
]

for user_id, recipient_id in users_recipients:
    payload = user_messages.generate_message(user_id, recipient_id)
    producer.send(
        topic="messages",
        key=str(payload["user_id"]),
        value=payload
    )

    producer.flush()