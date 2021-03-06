from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.linalg import Vectors, DenseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans

from pyspark.sql import SQLContext, Row

import time, sched, json
from datetime import datetime
import uuid
import os

from configparser import ConfigParser
config = ConfigParser()
config.read('../configuration.ini')

print("START PROGRAM")

BATCH_TIME = 10

cassandra_host = config.get('CONFIG', 'cassandra_host')
cassandra_port = config.get('CONFIG', 'cassandra_port')
cassandra_keyspace = config.get('CONFIG', 'cassandra_keyspace')
cassandra_table = config.get('CONFIG', 'cassandra_table')

spark_master = config.get('CONFIG', 'spark_master')

zk = config.get('CONFIG', 'zookeeper')

topic = config.get('CONFIG', 'kafka_topic')
brokers = config.get('CONFIG', 'kafka_brokers')

conf = SparkConf().setAppName("K-Means Spark").setMaster(spark_master).set("spark.cassandra.connection.host", cassandra_host).set("spark.cassandra.connection.port", cassandra_port).set("park.cassandra.connection.keep_alive_ms", 10000)
sc = SparkContext.getOrCreate(conf=conf)
ssc = StreamingContext(sc, BATCH_TIME)
sqlContext = SQLContext(sc)

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

raw = kvs.flatMap(lambda kafkaS: [kafkaS])
lines = raw.map(lambda xs: json.loads(xs[1]))

parsed_db = lines.map(lambda x: {"id": str(uuid.uuid4()), "latitude": float(x['latitude']), "longitude": float(x['longitude']),'timestamp':datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"type":"COORD"})

def saveToDB(rdd):
    if not rdd.isEmpty():
        df = sqlContext.createDataFrame(rdd)
        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table=cassandra_table, keyspace=cassandra_keyspace)\
            .save()
parsed_db.foreachRDD(lambda x: saveToDB(x))

lines = lines.map(lambda x: x['latitude'] + " " + x['longitude'])
lines = lines.map(lambda x: x.split(" "))
lines = lines.map(lambda x: DenseVector(x))

lines.pprint()

initCenters = [[33.0, -96.0], [34.0, -97.0], [35.0, -98.0]]
model = StreamingKMeans(k=3, decayFactor=0.01).setInitialCenters(initCenters, [1.0, 1.0, 1.0])
model.trainOn(lines)

ssc.start()

s = sched.scheduler(time.time, time.sleep)

def print_cluster_centers(scx, model):
    uid = str(uuid.uuid4())
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    centers = model.latestModel().centers
    for center in centers:
        rdd = sc.parallelize([{
            "id": str(uuid.uuid4()),
            "latitude": float(center[0]),
            "longitude": float(center[1]),
            "timestamp": t,
            "type":'CENTER',
            "centers_uuid": uid
        }])
        df = sqlContext.createDataFrame(rdd)
        df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="points", keyspace="traffickeyspace")\
            .save()

    s.enter(BATCH_TIME, 1, print_cluster_centers, (scx, model))
s.enter(BATCH_TIME, 1, print_cluster_centers, (s, model))
s.run()

ssc.awaitTermination()

