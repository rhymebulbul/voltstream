# VoltStream: Real-Time Smart Grid Analytics Platform

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Spark](https://img.shields.io/badge/apache%20spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/apache_kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

VoltStream is a production-grade, end-to-end predictive analytics platform designed for high-throughput IoT telemetry. It ingests independent, high-velocity sensor streams, engineers temporal features at scale, and executes real-time machine learning inference to forecast energy demand.

## ⚡ Architecture & Engineering Highlights

### 1. High-Performance Data Engineering (Bypassing the GIL)
Processing millions of raw IoT telemetry records is inherently CPU-bound. VoltStream implements multi-core parallel processing patterns using `multiprocessing.Pool` combined with vectorized C-level NumPy and Pandas operations. 
* **Impact**: Successfully circumvents the Python Global Interpreter Lock (GIL) bottlenecks, achieving throughputs of **>600,000 rows/sec** on commodity hardware during the initial 14-million-row historical data aggregation. Output is partitioned into PyArrow `.parquet` files for optimized downstream PySpark reads.

### 2. Distributed Machine Learning (PySpark MLlib)
Engineered a distributed **Gradient Boosted Tree (GBT) Regression** pipeline to predict aggregate energy consumption based on weather semantics and building characteristics.
* **Pipeline**: Utilizes a combination of `StringIndexer`, `OneHotEncoder`, and `StandardScaler` to map categorical constraints and scale continuous variance.
* **Persistence**: The tuned PySpark `PipelineModel` is serialized and persisted to disk for zero-latency loading in the streaming tier.

### 3. Real-Time Event Processing (Spark Structured Streaming & Kafka)
Architected an autonomous, real-time inference engine. 
* **IoT Simulation**: A Python Kafka producer simulates an independent, unbounded stream of live IoT building sensor telemetry.
* **Temporal Time-Window Joins**: The Spark Structured Streaming engine consumes the Kafka topics and executes sliding time-window joins across the independent streams (Telemetry + Weather) utilizing event-time `.withWatermark()` constraints to handle late-arriving data without memory leaks.
* **Live Inference**: The persisted GBT pipeline is applied directly to the joined streaming DataFrame, publishing sub-second predictions to downstream consumers.

---

## 🛠️ Repository Structure
* `src/data_engineering.py` — Multi-core feature engineering and Parquet generation.
* `src/model_training.py` — PySpark MLlib distributed model training.
* `src/streaming_job.py` — Spark Structured Streaming engine and temporal joins.
* `src/producer.py` & `src/consumer.py` — Kafka IoT simulator and prediction consumer.
* `data/` & `models/` — Local storage for Parquet datasets and serialized pipeline models.

---

## 🚀 Quickstart Guide

### 1. Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Infrastructure (Kafka Cluster)
Spin up the local Confluent Kafka and Zookeeper brokers:
```bash
docker-compose up -d
```

### 3. Execution Pipeline
Run the platform sequentially to build the data, train the model, and launch the real-time streams:

```bash
# 1. Process 14M raw historical records into optimized Parquet partitions (~25 seconds)
python3 src/data_engineering.py

# 2. Train and persist the distributed PySpark GBT model
python3 src/model_training.py

# 3. Launch the Real-Time Streaming Inference Engine
python3 src/streaming_job.py
```

In a separate terminal, start the IoT Simulator to pump live data into Kafka:
```bash
source .venv/bin/activate
python3 src/producer.py
```
*(Optional) Watch the predictions live via the consumer:*
```bash
python3 src/consumer.py
```
