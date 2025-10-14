import websocket
import json
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 5,
    'delivery.timeout.ms': 60000,
    'linger.ms': 5,
    'compression.type': 'snappy', 
}

producer = Producer(conf)


# Kafka Topic
TOPIC_NAME = 'real_time'

# Explicit partition mapping to guarantee 1 coin = 1 partition
PARTITION_MAP = {
    'BTC-USDT': 0,
    'ETH-USDT': 1,
    'SOL-USDT': 2,
    'XRP-USDT': 3,
    'ADA-USDT': 4
}

def on_message(ws, message):
    try:
        data = json.loads(message)
        
       # Print the received message
        print(f"\n[MESSAGE RECEIVED] {json.dumps(data, indent=2)}") 

        # Prepare Kafka message parameters
        kafka_params = {'topic': TOPIC_NAME, 'value': json.dumps(data).encode('utf-8')}
        
        # If ticker data, add key and partition
        if 'data' in data:
            ticker = data['data'][0].get('instId')
            kafka_params['key'] = ticker.encode('utf-8')
            kafka_params['partition'] = PARTITION_MAP.get(ticker)
        
        # Produce to Kafka  
        producer.produce(**kafka_params)
        producer.poll(0)
    except Exception as e:
        print("Error processing WebSocket message:", e)

def on_open(ws):
    print("WebSocket opened")
    subscribe_msg = {
        "op": "subscribe",
        "args": [
            {"channel": "tickers", "instId": pair}
            for pair in PARTITION_MAP.keys()
        ]
    }
    ws.send(json.dumps(subscribe_msg))

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, *args):
    print("WebSocket closed")
    producer.flush()  # Ensure all messages are delivered before exit

if __name__ == "__main__":
    ws = websocket.WebSocketApp("wss://ws.okx.com:8443/ws/v5/public",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()