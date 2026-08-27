# VoltStream: Smart Grid Predictive Analytics Pipeline

VoltStream is an end-to-end predictive analytics platform for IoT building meters and weather telemetry data. It combines multi-core data optimization, distributed machine learning, and real-time event processing.

## Key Features
* **Distributed Machine Learning**: Engineered a distributed Gradient Boosted Tree (GBT) regression model using PySpark MLlib on large-scale weather and telemetry data, persisting tuned Pipeline Models for high-throughput inference.
* **Real-Time Streaming**: Architected a real-time event processing engine using Spark Structured Streaming and Kafka, executing temporal time-window joins across 3 independent, high-velocity sensor streams.
* **High-Performance Compute**: Optimized CPU-bound workloads by implementing multi-core parallel processing patterns and vectorized NumPy/Pandas operations, circumventing GIL bottlenecks for large-scale aggregations.

## Repository Structure
* `src/`: Core pipeline notebooks for data processing, model training, and Kafka streaming.
* `data/`: Sample telemetry datasets (meters, weather, building information).
* `models/`: Persisted PySpark PipelineModels.
