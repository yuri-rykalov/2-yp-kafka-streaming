import kafka_app.logger

from kafka_app.admin import KafkaAdmin 
from kafka_app.producer import KafkaProducer
from utils.events_store import EventsStore


# Запустим Producer и отправим сообщения
producer = KafkaProducer("localhost:9094")
events_store = EventsStore()

test_payload = events_store.get_event("00")

producer.send(
    topic="user_events",
    key=str(test_payload["sender_id"]),
    value=test_payload
)

producer.flush()