import time
import datetime as dt


class UserMessages:

    def generate_message(self, user_id: int, recipient_id: int):
        """
        Receives user_id and recipient_id as integers
        Generates message payload
        """

        payload = {
            "user_id": user_id,
            "recipient_id": recipient_id,
            "timestamp": int(time.time()),
            "message": (
                f"Message from {user_id} to {recipient_id} "
                f"at: {dt.datetime.fromtimestamp(time.time())}"
            )
        }

        return payload