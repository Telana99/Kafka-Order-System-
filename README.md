# Kafka Order Processing System with Avro

This project is a demonstration of a real-time data processing system built with Apache Kafka. It implements a producer that generates simulated order messages and a consumer that processes them, featuring Avro serialization, real-time aggregation, retry logic, and a Dead Letter Queue (DLQ).

## 🚀 Features

- **Avro Serialization**: Order messages are serialized using Avro for efficient, schema-enabled data exchange.
- **Real-time Aggregation**: The consumer calculates a running average of order prices.
- **Fault Tolerance**:
  - **Retry Logic**: Temporarily failed messages are retried a configurable number of times.
  - **Dead Letter Queue (DLQ)**: Permanently failed messages after retries are sent to a dedicated DLQ topic for further inspection.
- **Language Agnostic**: The implementation can be demonstrated in any programming language (e.g., Java, Python, Go).

## 🏗️ System Architecture

The system consists of the following core components:

1.  **Order Producer**: Generates mock order data and publishes it to the main Kafka topic.
2.  **Order Consumer**: Consumes messages from the main topic, performs aggregation, and handles failures.
3.  **Kafka Topics**:
    - `orders`: The main topic for order messages.
    - `orders-dlq`: The Dead Letter Queue for failed messages.
4.  **Schema Registry**: (Required for Avro) Manages and stores the Avro schema for order messages.

## 🛠️ Prerequisites

Before running this project, ensure you have the following installed:

-  **Python 3.7+** 
- **Apache Kafka** 

