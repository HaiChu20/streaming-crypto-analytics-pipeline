import websocket
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka Topic
TOPIC_NAME = 'BTC_analysis'

def on_message(ws, message):
    try:
        data = json.loads(message)
        producer.send(TOPIC_NAME, value=data)
        print("Sent to Kafka:", data)

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

if __name__ == "__main__":
    ws = websocket.WebSocketApp("wss://ws.okx.com:8443/ws/v5/public",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()