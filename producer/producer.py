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
TOPIC_NAME = 'CryptoCurrency_analysis'

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Print the received message
        print(f"\n[MESSAGE RECEIVED] {json.dumps(data, indent=2)}")
        
        # Produce message to Kafka
        producer.produce(
            topic=TOPIC_NAME,
            value= json.dumps(data).encode('utf-8')
        )
        # Trigger sending of queued messages
        producer.poll(0)
    except Exception as e:
        print("Error processing WebSocket message:", e)

def on_open(ws):
    print("WebSocket opened")
    subscribe_msg = {
        "op": "subscribe",
        "args": [
            {
                "channel": "tickers",
                "instId": "BTC-USDT"
            }
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