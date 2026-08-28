import json
from kafka import KafkaConsumer

def main():
    print("Starting Predictions Consumer...")
    consumer = KafkaConsumer(
        "predictions_stream",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Listening for real-time predictions...")
    try:
        for message in consumer:
            data = message.value
            print(f"[Real-Time Inference] Building: {data.get('building_id', 'N/A')} | Predicted Energy: {data.get('prediction', 0.0):.4f}")
    except KeyboardInterrupt:
        print("Stopping Consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
