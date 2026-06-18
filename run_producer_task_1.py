import kafka_app.logger
import time

from kafka_app.admin import KafkaAdmin 
from kafka_app.producer import KafkaProducer
from utils.events_store import EventsStore


# Запустим Producer и отправим сообщения
producer = KafkaProducer("localhost:9094")

# Запустим test run, чтобы проверить filtering сообщений
events_store = EventsStore()

# Test message routing
blocking_user_steps = [
    "00",   # Тестовое сообщение -> видим в user_events
    "01", # Сообщение от NOT blocked user -> filtered_messages
    "02", # block user
    "03", # Сообщение от blocked user -> blocked messages
    "04", # unblock user
    "05", # Сообщение от unblocked user -> filtered messages
]

for step in blocking_user_steps:
    payload = events_store.get_event(step)

    producer.send(
        topic="user_events",
        key=str(payload["recipient_id"]),
        value=payload
    )

    producer.flush()

# Test censoring messages

# Добавить слово в бан
producer.send(
    topic="banned_words",
    key=str(payload["event_type"]),
    value=events_store.get_event("06")
)
producer.flush()

# wait 5 seconds
time.sleep(5)

# Отправить сообщение с забаненным словом -> censored у получателя
producer.send(
    topic="user_events",
    key=str(payload["recipient_id"]),
    value=events_store.get_event("07")
)
producer.flush()

# Разбанить слово
producer.send(
    topic="banned_words",
    key=str(payload["event_type"]),
    value=events_store.get_event("08")
)
producer.flush()

# wait 5 seconds
time.sleep(5)

# Отправить сообщение с забаненным словом -> оригинал у получателя
producer.send(
    topic="user_events",
    key=str(payload["recipient_id"]),
    value=events_store.get_event("09")
)
producer.flush()