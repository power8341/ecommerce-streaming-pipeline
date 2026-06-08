# E-commerce Real-Time Streaming Pipeline
> Real-time order processing using Kafka, PySpark Structured Streaming, and Delta Lake

## Architecture
Python Producer (Faker)
↓
Kafka Topic: ecommerce-orders
↓
PySpark Structured Streaming
↓
Delta Lake (/tmp/delta/ecommerce_orders)
↓
SQL Analytics & Aggregations
## Tech Stack
| Tool | Purpose |
|------|---------|
| Apache Kafka | Message queue for real-time order events |
| PySpark Structured Streaming | Stream processing engine |
| Delta Lake | Reliable, versioned data storage |
| Python / Faker | Order event simulation |
| Docker | Kafka + Zookeeper local setup |

## Pipeline Components
| File | Description |
|------|-------------|
| `producer/order_producer.py` | Simulates live e-commerce orders into Kafka |
| `consumer/streaming_consumer.py` | PySpark stream reader, writes to Delta Lake |
| `notebooks/delta_query.py` | Analytics queries on captured orders |
| `docker-compose.yml` | Kafka + Zookeeper setup |

## Key Results
- ⚡ Orders processed every **5 seconds** via micro-batch streaming
- 📦 **156 orders** captured in a single pipeline run
- 🏆 **Electronics** top category — $114,900 revenue
- 🌍 Orders simulated across **multiple countries**
- ✅ Full **schema enforcement** and **Delta Lake versioning**

## How to Run

### 1. Start Kafka
```bash
docker-compose up -d
```

### 2. Start the producer (new terminal)
```bash
python3 producer/order_producer.py
```

### 3. Start the streaming consumer (new terminal)
```bash
python3 consumer/streaming_consumer.py
```

### 4. Query the Delta Lake results (new terminal)
```bash
python3 notebooks/delta_query.py
```

## Requirements
- Docker Desktop
- Python 3.11+
- Java 17
- PySpark 4.1.1

```bash
pip install kafka-python faker pyspark delta-spark
```

## Author
**Sai Teja Eleti** — Data Engineer
