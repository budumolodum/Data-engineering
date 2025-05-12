import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time


# retry loop
for _ in range(10):
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
    TOPIC = os.getenv("TOPIC_NAME", "topic1")
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group'
        )
        break
    except NoBrokersAvailable:
        print("Kafka not available for consumer, retrying...")
        time.sleep(5)
else:
    raise Exception("Kafka brokers not available after 10 tries")

print(f"Consumer started for topic: {TOPIC}")
for msg in consumer:
    print(f"[{TOPIC}] {msg.value.decode('utf-8')}")
