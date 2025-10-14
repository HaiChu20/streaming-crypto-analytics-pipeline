"""
Flink Job: Real-Time Crypto Data Transformation
Reads from Kafka 'real_time' topic → Transforms → Writes to 'processed_data' topic
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
import json
from datetime import datetime

class CryptoTransformer(MapFunction):
    """Transform raw crypto ticker data"""
    
    def map(self, value):
        try:
            data = json.loads(value)
            
            # Extract ticker info
            if 'data' in data and len(data['data']) > 0:
                ticker_data = data['data'][0]
                
                # Calculate additional metrics
                last_price = float(ticker_data.get('last', 0))
                open_24h = float(ticker_data.get('open24h', 0))
                high_24h = float(ticker_data.get('high24h', 0))
                low_24h = float(ticker_data.get('low24h', 0))
                volume_24h = float(ticker_data.get('vol24h', 0))
                
                # Calculate price change percentage
                price_change_pct = ((last_price - open_24h) / open_24h * 100) if open_24h > 0 else 0
                
                # Calculate price volatility (high-low spread)
                volatility_pct = ((high_24h - low_24h) / low_24h * 100) if low_24h > 0 else 0
                
                # Enriched data
                transformed = {
                    'symbol': ticker_data.get('instId'),
                    'timestamp': datetime.fromtimestamp(int(ticker_data.get('ts', 0)) / 1000).isoformat(),
                    'price': last_price,
                    'open_24h': open_24h,
                    'high_24h': high_24h,
                    'low_24h': low_24h,
                    'volume_24h': volume_24h,
                    'volume_usd_24h': float(ticker_data.get('volCcy24h', 0)),
                    'price_change_24h_pct': round(price_change_pct, 2),
                    'volatility_24h_pct': round(volatility_pct, 2),
                    'bid_price': float(ticker_data.get('bidPx', 0)),
                    'ask_price': float(ticker_data.get('askPx', 0)),
                    'bid_size': float(ticker_data.get('bidSz', 0)),
                    'ask_size': float(ticker_data.get('askSz', 0)),
                    'processed_at': datetime.now().isoformat()
                }
                
                return json.dumps(transformed)
            
            return value
            
        except Exception as e:
            print(f"Error transforming data: {e}")
            return value

def create_flink_job():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)  # Match our 4 task slots
    
    # Kafka source configuration
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092') \
        .set_topics('real_time') \
        .set_group_id('flink-consumer-group') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create data stream
    stream = env.from_source(
        kafka_source,
        watermark_strategy=None,
        source_name='Kafka Source'
    )
    
    # Transform data
    transformed_stream = stream.map(
        CryptoTransformer(),
        output_type=Types.STRING()
    )
    
    # Kafka sink configuration
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic('processed_data')
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    
    # Write to Kafka
    transformed_stream.sink_to(kafka_sink)
    
    # Execute
    env.execute('Crypto Data Transformation')

if __name__ == '__main__':
    create_flink_job()
