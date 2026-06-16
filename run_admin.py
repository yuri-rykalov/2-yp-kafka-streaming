import kafka_app.logger

from kafka_app.admin import KafkaAdmin


admin = KafkaAdmin("localhost:9094")

admin.create_topics(
    topics = [
        "user_events",       # События: сообщения, блокирвка / разблокировка пользователя
        "blocked_users",     # Заблокированные пользователи
        "filtered_messages", # Сообщения, доставленные адресату
        "blocked_messages",   # Сообщения от заблокированных пользователей
        "banned_words"       # Забаненные слова
    ],
    partitions=3,
    replicas=2
)
