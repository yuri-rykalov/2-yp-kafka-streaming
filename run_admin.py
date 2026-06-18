import kafka_app.logger

from kafka_app.admin import KafkaAdmin


admin = KafkaAdmin("localhost:9094")

admin.create_topics(
    topics = [
        "user_events",       # Task 1: События: сообщения, блокирвка / разблокировка пользователя
        "blocked_users",     # Task 1: Заблокированные пользователи
        "filtered_messages", # Task 1: Сообщения, доставленные адресату
        "blocked_messages",  # Task 1: Сообщения от заблокированных пользователей
        "banned_words",      # Task 1: Забаненные слова
        "messages"           # Task 2: Сообщения пользователей для обработки в ksqlDB
    ],
    partitions=3,
    replicas=2
)
