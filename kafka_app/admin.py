import logging

from confluent_kafka.admin import AdminClient, NewTopic
from typing import List


logger = logging.getLogger(__name__)

class KafkaAdmin:

    def __init__(self, bootstrap_servers):
        self.admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def create_topics(
            self,
            topics: List[str],
            partitions: int = 3,
            replicas: int = 2
        ):
        """
        Creates Kafka topics from a given 
        topic names (list of strings), 
        nb of partitions (int) - 3 by default 
        and nb of replicas (int) - 2 by default
        """

        # Define topic parameters
        new_topics = [
            NewTopic(
                topic,                         # Topic name
                num_partitions = partitions,   # Number of partitions
                replication_factor = replicas, # Number of replicas for each partition
            )
            for topic in topics
        ]

        futures = self.admin.create_topics(new_topics)

        # Create topics in Kafka
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic '{topic}' created successfully")
            except Exception as e:
                logger.error(f"Topic '{topic}' failes: {e}")
