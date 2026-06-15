import kafka_app.logger

from kafka_app.admin import KafkaAdmin


admin = KafkaAdmin("localhost:9094")

admin.create_topics(
    topics = [
        "messages",
        "filtered_messages",
        "blocked_users",
        "blocked_messages"
    ],
    partitions=3,
    replicas=2
)