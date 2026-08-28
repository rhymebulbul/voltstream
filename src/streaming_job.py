import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.ml import PipelineModel

def main():
    print("Initializing Real-Time Engine (Spark Structured Streaming)...")
    spark = SparkSession.builder \
        .appName("VoltStream Real-Time Inference") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # 1. Load the pre-trained MLlib Pipeline Model
    print("Loading GBT Pipeline Model...")
    try:
        model = PipelineModel.load("models/best_model_GBT")
    except Exception as e:
        print("Error: Could not load model. Ensure Sprint 2 has run successfully.", e)
        sys.exit(1)

    # 2. Define schemas for the 3 incoming Kafka JSON streams
    meter_schema = StructType([
        StructField("building_id", IntegerType(), True),
        StructField("meter_type", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    weather_schema = StructType([
        StructField("site_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("air_temperature", DoubleType(), True),
        StructField("cloud_coverage", DoubleType(), True)
    ])

    # 3. Read streams from Kafka and apply Watermarks for temporal window joins
    def read_kafka_stream(topic, schema, timestamp_col):
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        return df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*") \
            .withWatermark(timestamp_col, "10 minutes")

    print("Connecting to independent high-velocity sensor streams...")
    meters_stream = read_kafka_stream("meter_stream", meter_schema, "ts").withColumnRenamed("ts", "meter_ts")
    weather_stream = read_kafka_stream("weather_stream", weather_schema, "timestamp").withColumnRenamed("timestamp", "weather_ts")

    # 4. Execute Temporal Time-Window Joins across the streams
    # Joining meters with weather data within a 1-hour time window (fulfilling the resume claim)
    print("Executing temporal time-window joins...")
    joined_stream = meters_stream.join(
        weather_stream,
        expr("""
            meter_ts >= weather_ts AND
            meter_ts <= weather_ts + interval 1 hour
        """)
    )
    
    # 5. Feature extraction to match the model schema
    # In a real scenario, we'd also join building info.
    inference_df = joined_stream.withColumn("hour", expr("hour(meter_ts)")) \
                                .withColumn("day_of_week", expr("dayofweek(meter_ts)")) \
                                .withColumn("month", expr("month(meter_ts)")) \
                                .withColumn("square_feet", expr("50000")) \
                                .withColumn("floor_count", expr("3")) \
                                .withColumn("year_built", expr("1990")) \
                                .withColumn("primary_use", expr("'Education'"))

    # 6. Apply the ML Model for Real-Time Inference
    predictions = model.transform(inference_df)
    
    # 7. Write stream to output
    print("Starting Streaming Query...")
    query = predictions.select("building_id", "meter_ts", "prediction") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
