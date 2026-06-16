import json
import logging
from confluent_kafka import Producer


logger = logging.getLogger(__name__)

class KafkaProducer:

    def __init__(self, bootstrap_servers):
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",   # Sync replication
            "retries": 3     # Number of retries
        })

    @staticmethod
    def delivery_report(err, msg):
        """
        If delivery was successful prints: topic and partition where it has been sent
        Also prints offset
        If delivery failed - prints error 
        """

        # If delivery failed
        if err is not None:
            logger.warning(f"Message delivery failed: {err}")

        # If delivery was successful
        else:
            logger.info(
                f"Message delivered to "
                f"{msg.topic()} [{msg.partition()}] "
                f"at offset {msg.offset()}"
            )

    def send(self, topic, value, key=None):
        """
        Serializes payload value
        Sends message
        If delivery was successful prints delivery report
        If delivery failed prints error
        """

        try:
            payload = json.dumps(value)

            self.producer.produce(
                topic=topic,
                key=key,
                value=payload,
                callback=self.delivery_report
            )

            self.producer.poll(0)

        except Exception as e:
            logger.error(
                f"Serialization error as {e} "
                f"{value}"
                )
            
    def flush(self):
        self.producer.flush()