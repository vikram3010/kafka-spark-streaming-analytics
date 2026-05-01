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
