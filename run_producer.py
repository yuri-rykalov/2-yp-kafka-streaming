import kafka_app.logger

from kafka_app.admin import KafkaAdmin 
from kafka_app.producer import KafkaProducer
from utils.events_store import EventsStore


# Запустим Producer и отправим сообщения
producer = KafkaProducer("localhost:9094")

# Запустим test run, чтобы проверить filtering сообщений
events_store = EventsStore()

steps = [
    "00",   # Тестовое сообщение -> видим в user_events
    "01", # Сообщение от NOT blocked user -> filtered_messages
    "02", # block user
    "03", # Сообщение от blocked user -> blocked messages
    "04", # unblock user
    "05" # Сообщение от unblocked user -> filtered messages
]

for step in steps:
    payload = events_store.get_event(step)

    producer.send(
        topic="user_events",
        key=str(payload["recepient_id"]),
        value=payload
    )

    producer.flush()