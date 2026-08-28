import time
import json
import pandas as pd
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def main():
    print("Starting IoT Simulator (Kafka Producer)...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )
    
    # We simulate a live data stream by reading our raw meter data
    csv_path = 'data/meters.csv'
    print(f"Streaming data from {csv_path}...")
    
    # Stream line by line (simulating 100 events/sec)
    chunk_iter = pd.read_csv(csv_path, chunksize=100)
    
    try:
        for chunk in chunk_iter:
            records = chunk.to_dict(orient='records')
            for record in records:
                # push to meter_stream topic
                producer.send("meter_stream", value=record)
                
            producer.flush()
            # print status and sleep to simulate real-time
            print(f"Produced 100 events to 'meter_stream'...")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Stopping Producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
