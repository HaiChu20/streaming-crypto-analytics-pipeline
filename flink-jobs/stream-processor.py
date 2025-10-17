from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector


class SnowflakeSink(ProcessFunction):
    """Custom Snowflake sink for batch writes"""
    
    def __init__(self, batch_size=100, flush_interval=10):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.last_flush = datetime.now()
        
    def open(self, runtime_context):
        """Initialize Snowflake connection"""
        # Load environment variables from .env file
        # This needs to be in open() because PyFlink distributes this code to workers
        load_dotenv()
        
        self.conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        self.cursor = self.conn.cursor()
        
    def process_element(self, value, ctx):
        """Process each record and batch write to Snowflake"""
        if value is None:
            return
            
        try:
            data = json.loads(value)
            
            # Extract data from OKX ticker format
            if 'data' in data and len(data['data']) > 0:
                ticker_data = data['data'][0]
                
                # Transform to Snowflake record format
                record = {
                    'symbol': ticker_data.get('instId', 'UNKNOWN'),
                    'timestamp': datetime.fromtimestamp(int(ticker_data.get('ts', 0)) / 1000),
                    'price': float(ticker_data.get('last', 0)),
                    'open_24h': float(ticker_data.get('open24h', 0)),
                    'high_24h': float(ticker_data.get('high24h', 0)),
                    'low_24h': float(ticker_data.get('low24h', 0)),
                    'volume_24h': float(ticker_data.get('vol24h', 0)),
                    'volume_usd_24h': float(ticker_data.get('volCcy24h', 0)),
                    'price_change_24h_pct': float(ticker_data.get('sodUtc0', 0)),
                    'bid_price': float(ticker_data.get('bidPx', 0)),
                    'ask_price': float(ticker_data.get('askPx', 0)),
                    'bid_size': float(ticker_data.get('bidSz', 0)),
                    'ask_size': float(ticker_data.get('askSz', 0)),
                    'processed_at': datetime.now()
                }
                
                self.buffer.append(record)
            
            # Flush if batch size reached or time elapsed
            current_time = datetime.now()
            time_elapsed = (current_time - self.last_flush).seconds
            
            if len(self.buffer) >= self.batch_size or time_elapsed >= self.flush_interval:
                self.flush_buffer()
                
        except Exception as e:
            print(f"[ERROR] Process element failed: {e}")
    
    def flush_buffer(self):
        """Write buffered records to Snowflake"""
        if not self.buffer:
            return
            
        try:
            # Prepare batch insert
            insert_query = """
            INSERT INTO crypto_ticker_data (
                symbol, timestamp, price, open_24h, high_24h, low_24h,
                volume_24h, volume_usd_24h, price_change_24h_pct, bid_price,
                ask_price, bid_size, ask_size, processed_at
            ) VALUES (
                %(symbol)s, %(timestamp)s, %(price)s, %(open_24h)s, %(high_24h)s, %(low_24h)s,
                %(volume_24h)s, %(volume_usd_24h)s, %(price_change_24h_pct)s, %(bid_price)s,
                %(ask_price)s, %(bid_size)s, %(ask_size)s, %(processed_at)s
            )
            """
            
            # Execute batch insert
            self.cursor.executemany(insert_query, self.buffer)
            self.conn.commit()
            
            print(f"[INFO] Flushed {len(self.buffer)} records to Snowflake")
            
            # Clear buffer
            self.buffer = []
            self.last_flush = datetime.now()
            
        except Exception as e:
            print(f"[ERROR] Snowflake write failed: {e}")
            self.conn.rollback()
    
    def close(self):
        """Flush remaining records and close connection"""
        self.flush_buffer()
        self.cursor.close()
        self.conn.close()
        print("[INFO] Snowflake connection closed")
