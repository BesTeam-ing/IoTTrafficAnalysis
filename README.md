# IoTTrafficAnalysis
IoT Traffic Analysis and Simulator with Kafka, Spark, Flask and Cassandra DB

# System requirements
JDK - 1.8

ZooKeeper - 3.4.13

Kafka - 2.4.0

Cassandra - 3.11.10

Spark - 2.3.4

Scala - 2.11.8

Flask - 2.0.1

Python - 3.7

# Run

```
python -m venv venv
source venv/bin/activation
pip install -r requirements
```

Producer
```
cd Kafka-Producer
python kafka-producer.py
```

Processor
```
cd Spark-Processor

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --jars pyspark-cassandra-2.3.0.jar --py-files pyspark-cassandra-2.3.0.jar --conf spark.cassandra.connection.host=localhost processor.py
```

Frontend
```
cd Flask-RealTime
python main
```
