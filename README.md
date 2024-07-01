# Building a predictive model for arrival time estimation for traffic vehicles

## Data

## Model

## Streaming
### Technologis
- Apache Kafka
- Apache Spark Structured Streaming
- MongoDB
### Requirements
- Kafka 3.6.0
- Spark 3.5.0
- MongoDB >= 10.2.0

### How to run
- **Step 1**: Start zookeeper server and kafka server
Code:
  - Start zookeeper server\
  `bin/zookeeper-server-start.sh config/zookeeper.properties`

  - Start kafka server\
  `bin/kafka-server-start.sh config/server.properties`

- **Step 2**: Start MongoDB
- **Step 3**: Start mongo_consumer, producer and consumer
  - Mongo_consumer\
    `python mongo_consumer.py`
    
  - Producer\
    `python producer.py`

  - Consumer\
    `python consumer.py`

