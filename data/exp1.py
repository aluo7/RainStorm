from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time

spark = SparkSession.builder.appName("GPlusExp1Streaming").getOrCreate()
sc = spark.sparkContext

ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("fa25-cs425-8501.cs.illinois.edu", 9999)

def parse_line(line):
    try:
        userid, feats = line.strip().split(",", 1)
        feat_list = feats.split("|")
        return (userid, feat_list)
    except:
        return None

parsed = lines.map(parse_line).filter(lambda x: x is not None)

def contains_new_jersey(features):
    for f in features:
        if "New Jersey" in f:
            return True
    return False

filtered = parsed.filter(lambda x: contains_new_jersey(x[1]))
mapped = filtered.map(lambda x: ("New Jersey", 1))
counts = mapped.reduceByKey(lambda a, b: a + b)

counts.pprint()

def save_rdd(rdd):
    if not rdd.isEmpty():
        rdd.saveAsTextFile("/home/sameern3/spark_job_outputs/experiment1")

counts.foreachRDD(save_rdd)

ssc.start()
ssc.awaitTermination()
