from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.linalg import Vectors, DenseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans

import pyspark_cassandra

import time, sched, json
from datetime import datetime
import uuid

print("START PROGRAM")

BATCH_TIME = 30
cassandra_host = "localhost"
cassandra_port = "9042"

conf = SparkConf().setAppName("K-Means Spark").setMaster("local[*]").set("spark.cassandra.connection.host", cassandra_host).set("spark.cassandra.connection.port", cassandra_port).set("park.cassandra.connection.keep_alive_ms", 10000)
sc = SparkContext.getOrCreate(conf=conf)
ssc = StreamingContext(sc, BATCH_TIME)

topic="iot-data-event"
zk="localhost:2181"
brokers="localhost:9092"

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

raw = kvs.flatMap(lambda kafkaS: [kafkaS])
lines = raw.map(lambda xs: json.loads(xs[1]))

parsed_db = lines.map(lambda x: {"id": str(uuid.uuid4()), "latitude": x['latitude'], "longitude": x['longitude'],'timestamp':datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"type":"COORD"})
parsed_db.foreachRDD(lambda x: x.saveToCassandra("traffickeyspace", "points"))

lines = lines.map(lambda x: x['latitude'] + " " + x['longitude'])

lines = lines.map(lambda x: x.split(" "))
lines = lines.map(lambda x: DenseVector(x))

lines.pprint()

#model = StreamingKMeans(k=3, decayFactor=0.01).setRandomCenters(2, 1.0, 0)
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
        rdd.saveToCassandra("traffickeyspace", "points", {"id", "latitude", "longitude", "timestamp", "type","centers_uuid"})
    
    s.enter(BATCH_TIME, 1, print_cluster_centers, (scx, model))
s.enter(BATCH_TIME, 1, print_cluster_centers, (s, model))
s.run()

ssc.awaitTermination()

