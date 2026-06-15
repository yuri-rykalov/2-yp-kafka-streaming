import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),               # Writes in terminal
        logging.FileHandler("kafka_app.log")   # Writes in file
    ]
)