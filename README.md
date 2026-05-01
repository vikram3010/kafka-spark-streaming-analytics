# Real-Time Streaming Analytics Pipeline with Kafka and Spark

This project implements a real-time streaming analytics pipeline using Apache Kafka and Spark Structured Streaming. The system simulates event-style sensor data, publishes messages into Kafka, consumes the stream using Spark, applies streaming transformations, and validates processed output in a reproducible workflow.

## Project Overview

Real-time data systems are used in monitoring, IoT, financial analytics, fraud detection, observability, and operational dashboards. This project demonstrates the core architecture of a streaming data pipeline:

- Event producer
- Kafka topic
- Spark Structured Streaming consumer
- Streaming transformations
- Micro-batch processing
- Output validation

## Architecture

```text
Sensor/Event Producer
        ↓
Apache Kafka Topic
        ↓
Spark Structured Streaming Consumer
        ↓
Streaming Transformations
        ↓
Processed Output / Console / Files
```

## Key Features

* Built a Kafka producer to publish streaming event data
* Configured Kafka topic-based message ingestion
* Used Spark Structured Streaming to consume and process real-time messages
* Applied schema parsing and streaming transformations
* Validated output using micro-batch execution
* Documented reproducible setup and execution flow

## Tech Stack

* Python
* Apache Kafka
* Apache Spark
* Spark Structured Streaming
* PySpark
* Data Engineering
* Big Data Systems
* Streaming Analytics

## Repository Structure

```text
kafka-spark-streaming-analytics/
├── README.md
├── requirements.txt
├── producer/
├── spark_streaming/
├── data_sample/
├── outputs/
└── reports/
```

## Skills Demonstrated

* Real-time data processing
* Kafka producer-consumer architecture
* Spark Structured Streaming
* PySpark development
* Big data pipeline design
* Stream processing concepts
* Reproducible technical documentation

## Status

This repository is being organized as part of my data engineering and big data portfolio. Code, setup steps, and output examples will be added in a clean reproducible format.
