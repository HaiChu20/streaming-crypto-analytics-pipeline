import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'BTC_analysis',
    bootstrap_servers='localhost:9092',
    group_id='moving_avg_group',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print("Received message:", message.value)