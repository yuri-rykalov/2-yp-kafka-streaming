import logging
import faust


logger = logging.getLogger(__name__)

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
    sender_id: str
    recepient_id: str
    message_id: str = None
    text: str = None

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

# Define table which stores blocked users
blocked_users = app.Table(
    "blocked_users",
    default=list,
    key_type=str,
    value_type=list,
)

# Define Agent
@app.agent(user_events_topic)
async def process_user_events(events):
    async for recepient_id, event in events.items():

        blocked_list = blocked_users[recepient_id]

        # If event_type = block user
        if event.event_type == "block":
            if event.sender_id not in blocked_list:
                blocked_list.append(event.sender_id)

            blocked_users[recepient_id] = blocked_list
            logger.info(f"User '{event.sender_id}' blocked by '{recepient_id}'")

        # If event_type = unblock
        elif event.event_type == "unblock":
            if event.sender_id in blocked_list:
                blocked_list.remove(event.sender_id)

            blocked_users[recepient_id] = blocked_list
            logger.info(f"User '{event.sender_id}' unblocked by {recepient_id}")

        # If event_type = message
        elif event.event_type == "message":
            # Message from blocked user -> blocked_messages_topic
            if event.sender_id in blocked_list:
                await blocked_messages_topic.send(
                    key=recepient_id,
                    value=event,
                )
                logger.info(
                    f"Message '{event.message_id}' blocked: "
                    f"sender={event.sender_id}, recepient={recepient_id}"
                )

            # Message from NOT blocked user -> filtered_messages_topic
            else:
                await filtered_messages_topic.send(
                    key=recepient_id,
                    value=event,
                )
                logger.info(
                    f"Message '{event.message_id}' delivered: "
                    f"sender={event.sender_id}, recepient={recepient_id}"
                )

if __name__ == "__main__":
    app.main()

