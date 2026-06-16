class EventsStore:
    def __init__(self):
        self.events = {
            "00": {
                "event_type": "message",
                "message_id": "msg_001",
                "sender_id": "2001",
                "recipient_id": "1001",
                "text": "test message"
            }, 
            "01": {
                "event_type": "message",
                "message_id": "msg_002",
                "sender_id": "2001",
                "recipient_id": "1001",
                "text": "Step 1 - not yet blocked"
            },
            "02": {
                "event_type": "block",
                "sender_id": "2001",
                "recipient_id": "1001"
            }, 
            "03": {
                "event_type": "message",
                "message_id": "msg_003",
                "sender_id": "2001",
                "recipient_id": "1001",
                "text": "Step 3 - message from blocked user"
            },
            "04": {
                "event_type": "unblock",
                "sender_id": "2001",
                "recipient_id": "1001"
            },
            "05": {
                "event_type": "message",
                "message_id": "msg_004",
                "sender_id": "2001",
                "recipient_id": "1001",
                "text": "Step 5 - message from unblocked user"
            },
            "06": {
                "event_type": "ban_word",
                "word": "sucks"
            },
            "07": {
                "event_type": "message",
                "message_id": "msg_006",
                "sender_id": "2001",
                "recipient_id": "1001",
                "text": "Step 7 - Learning Kafka is sucks"
            },
            "08": {
                "event_type": "unban_word",
                "word": "sucks"
            },
            "09": {
                "event_type": "message",
                "message_id": "msg_007",
                "sender_id": "2001",
                "recipient_id": "1001",
                "text": "Step 9 - Being dump is sucks"
            }
        }

    def add_event(self, event: dict, event_key: str):
        self.events[event[event_key]] = event

    def get_event(self, event_key: str):
        return self.events.get(event_key)
