import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.ml import PipelineModel

def main():
    print("Initializing Real-Time Engine (Spark Structured Streaming)...")
    spark = SparkSession.builder \
        .appName("VoltStream Real-Time Inference") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # 1. Load the pre-trained MLlib Pipeline Model
    print("Loading GBT Pipeline Model...")
    try:
        model = PipelineModel.load("models/best_model_GBT")
    except Exception as e:
        print("Error: Could not load model. Ensure Sprint 2 has run successfully.", e)
        sys.exit(1)

    # 2. Define schema for the incoming Kafka JSON
    schema = StructType([
        StructField("building_id", IntegerType(), True),
        StructField("meter_type", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("row_id", IntegerType(), True)
    ])
    
    # 3. Read stream from Kafka
    print("Connecting to Kafka Topic 'meter_stream'...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "meter_stream") \
        .option("startingOffsets", "latest") \
        .load()
        
    # 4. Parse JSON
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
        
    # In a full implementation (Assignment 2B), this is where we would execute 
    # a temporal time-window join with the weather_stream.
    
    # 5. Apply the ML Model for Real-Time Inference
    # The pipeline model expects certain columns (e.g. square_feet, hour, etc.)
    # In production, we'd join with the dimension tables here.
    
    # predictions = model.transform(parsed_df)
    
    # 6. Write stream to output (Console for demo, or 'predictions_stream' Kafka topic)
    print("Starting Streaming Query...")
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
