class EventsStore:
    def __init__(self):
        self.events = {
            "00": {
                "event_type": "message",
                "message_id": "msg_001",
                "sender_id": "2001",
                "recipient_id": "1001",
                "text": "test message"
            }
        }

    def add_event(self, event: dict, event_key: str):
        self.events[event[event_key]] = event

    def get_event(self, event_key: str):
        return self.events.get(event_key)
