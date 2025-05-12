import os
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

for _ in range(10):
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS").split(',')
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: v.encode('utf-8'))
        break
    except NoBrokersAvailable:
        print("Kafka not available, retrying...")
        time.sleep(5)
else:
    raise Exception("Kafka brokers not available after 10 tries")
TOPICS = ["topic1", "topic2"]

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: v.encode('utf-8'))

df = pd.read_csv("data/Divvy_Trips_2019_Q4.csv")

for idx, row in df.iterrows():
    message = row.to_json()
    for topic in TOPICS:
        producer.send(topic, message)
    time.sleep(0.1)

producer.flush()
