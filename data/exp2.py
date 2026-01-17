from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("GPlusExp2Streaming").getOrCreate()
sc = spark.sparkContext

ssc = StreamingContext(sc, 1)  # 1-second batch interval

lines = ssc.socketTextStream("fa25-cs425-8501.cs.illinois.edu", 9999)

mapped = lines.map(lambda f: (f.strip(), 1)).filter(lambda x: x[0] != "")

counts = mapped.reduceByKey(lambda a, b: a + b)

def save_rdd(rdd):
    if not rdd.isEmpty():
        rdd.saveAsTextFile("/home/sameern3/spark_job_outputs/experiment2")

counts.foreachRDD(save_rdd)

counts.pprint()

ssc.start()
ssc.awaitTermination()
