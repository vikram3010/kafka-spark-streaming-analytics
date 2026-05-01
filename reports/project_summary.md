
# Project Summary

## Title

Real-Time Big Data Streaming Pipeline with Kafka and Spark Structured Streaming

## Objective

The goal of this project is to build a real-time data streaming pipeline that ingests event-style data through Apache Kafka and processes it using Spark Structured Streaming.

## Problem

Modern data systems often require continuous ingestion and processing of high-volume event streams. Batch processing is not sufficient for use cases such as monitoring, IoT analytics, fraud detection, observability, and real-time dashboards. This project demonstrates the core components of a streaming data architecture.

## System Architecture

```text
Sensor/Event Producer
        ↓
Apache Kafka Topic
        ↓
Spark Structured Streaming Consumer
        ↓
Schema Parsing and Transformations
        ↓
Windowed Aggregations
        ↓
Console / File Output
````

## Methodology

The project includes:

* Simulated sensor event generation
* Kafka producer implementation
* Kafka topic-based message publishing
* Spark Structured Streaming consumer implementation
* JSON schema parsing
* Event-time processing
* Watermarking
* Window-based aggregation
* Output validation through streaming micro-batches

## Key Implementation Details

The producer generates event messages containing:

* Event timestamp
* Sensor ID
* Temperature
* Humidity
* Pressure

The Spark consumer reads from Kafka, parses JSON messages, applies schema validation, groups events by time windows and sensor ID, and computes aggregate metrics such as average temperature, humidity, pressure, and event count.

## Skills Demonstrated

* Apache Kafka
* Spark Structured Streaming
* PySpark
* Real-time data engineering
* Event-driven architecture
* Streaming analytics
* Windowed aggregation
* Data pipeline design
* Reproducible technical documentation

## Status

This repository is organized as a portfolio version of a big data streaming project. It demonstrates the structure and core implementation of a Kafka + Spark streaming workflow.
