import logging
import faust

from utils.censoring import CensorMessages


logger = logging.getLogger(__name__)
censor = CensorMessages()

# Define Faust app
app = faust.App(
    "message-filter-app", # App name
    broker="kafka://localhost:9094", # Kafka broker
    store="memory://", # App storage
    topic_partitions=3, # Number of partitions
    topic_replication_factor=2, # Nb of replicas
)

class UserEvent(faust.Record, serializer="json"):
    event_type: str
    sender_id: str = None
    recipient_id: str = None
    message_id: str = None
    text: str = None
    word: str = None

class BannedWordEvent(faust.Record, serializer="json"):
    event_type: str
    word: str

class BlockedUsers(faust.Record, serializer="json"):
    sender_ids: list[str] = []

class BannedWords(faust.Record, serializer="json"):
    words: list[str] = []

# Define topics

# All events topic - inbound events
user_events_topic = app.topic(
    "user_events",
    key_type=str,
    value_type=UserEvent,
)

# Blocked messages topic - outbound
blocked_messages_topic = app.topic(
    "blocked_messages",
    key_type=str,
    value_type=UserEvent,
)

# Filtered messages topic - outbound
filtered_messages_topic = app.topic(
    "filtered_messages",
    key_type=str,
    value_type=UserEvent,
)

banned_words_topic = app.topic(
    "banned_words",
    key_type=str,
    value_type=BannedWordEvent,
)

# Define table which stores blocked users
blocked_users_table = app.Table(
    "blocked_users_table",
    default=BlockedUsers,
    key_type=str,
    value_type=BlockedUsers,
    partitions=3,
)

# Define table which stores banned words
banned_words_table = app.Table(
    "banned_words_table",
    default=BannedWords,
    key_type=str,
    value_type=BannedWords,
    partitions=3,
)

GLOBAL_BANNED_WORDS_KEY = "global"

# Define Agent - for censoring messages
@app.agent(banned_words_topic)
async def process_banned_words(events):
    async for key, event in events.items():
        if not event.word:
            logger.warning("Banned word event without word")
            continue

        banned_state = banned_words_table[GLOBAL_BANNED_WORDS_KEY] or BannedWords(words=[])
        banned_list = list(banned_state.words)

        word = event.word.lower()

        if event.event_type == "ban_word":
            if word not in banned_list:
                banned_list.append(word)

            banned_words_table[GLOBAL_BANNED_WORDS_KEY] = BannedWords(words=banned_list)
            logger.info(f"Banned word added: {word}")

        elif event.event_type == "unban_word":
            if word in banned_list:
                banned_list.remove(word)

            banned_words_table[GLOBAL_BANNED_WORDS_KEY] = BannedWords(words=banned_list)
            logger.info(f"Banned word removed: {word}")


# Define Agent - for blocking users and routing messages
@app.agent(user_events_topic)
async def process_user_events(events):
    async for recipient_id, event in events.items():

        # Blocking user
        if event.event_type == "block":
            blocked_state = blocked_users_table[recipient_id]
            blocked_list = list(blocked_state.sender_ids)

            if event.sender_id not in blocked_list:
                blocked_list.append(event.sender_id)

            blocked_users_table[recipient_id] = BlockedUsers(sender_ids=blocked_list)
            logger.info(f"User '{event.sender_id}' blocked by '{recipient_id}'")

        # Unblocking user
        elif event.event_type == "unblock":
            blocked_state = blocked_users_table[recipient_id] or BlockedUsers(sender_ids=[])
            blocked_list = list(blocked_state.sender_ids)

            if event.sender_id in blocked_list:
                blocked_list.remove(event.sender_id)

            blocked_users_table[recipient_id] = BlockedUsers(sender_ids=blocked_list)
            logger.info(f"User '{event.sender_id}' unblocked by {recipient_id}")

        # Censoring and routing messages
        elif event.event_type == "message":
            blocked_state = blocked_users_table[recipient_id] or BlockedUsers(sender_ids=[])
            blocked_list = list(blocked_state.sender_ids)

            banned_state = banned_words_table[GLOBAL_BANNED_WORDS_KEY] or BannedWords(words=[])
            banned_list = list(banned_state.words)

            # Censoring message text
            event.text = censor.censor_text(
                text=event.text,
                words=banned_list,
            )

            # Message from blocked user -> blocked_messages_topic
            if event.sender_id in blocked_list:
                await blocked_messages_topic.send(
                    key=recipient_id,
                    value=event,
                )
                logger.info(
                    f"Message '{event.message_id}' blocked: "
                    f"sender={event.sender_id}, recipient={recipient_id}"
                )

            # Message from NOT blocked user -> filtered_messages_topic
            else:
                await filtered_messages_topic.send(
                    key=recipient_id,
                    value=event,
                )
                logger.info(
                    f"Message '{event.message_id}' delivered: "
                    f"sender={event.sender_id}, recipient={recipient_id}"
                )

if __name__ == "__main__":
    app.main()

