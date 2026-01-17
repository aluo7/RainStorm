from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("WikicorpusExp3Streaming").getOrCreate()
sc = spark.sparkContext

ssc = StreamingContext(sc, 1)  # 1-second batch interval

# Connect to the TCP server streaming preprocessed CSV lines
lines = ssc.socketTextStream("fa25-cs425-8501.cs.illinois.edu", 9999)

# Parse CSV lines into (docid, text)
parsed = lines.map(lambda line: line.strip().split(",", 1)).filter(lambda x: len(x) == 2)

# Filter documents containing "poet"
filtered = parsed.filter(lambda x: "poet" in x[1].lower())

# Map for counting
mapped = filtered.map(lambda x: ("poet_docs", 1))
counts = mapped.reduceByKey(lambda a, b: a + b)

# Save output incrementally
def save_rdd(rdd):
    if not rdd.isEmpty():
        rdd.map(lambda x: ",".join([str(x[0]), str(x[1])])).saveAsTextFile("/home/sameern3/spark_job_outputs/experiment3")

counts.foreachRDD(save_rdd)

counts.pprint()

ssc.start()
ssc.awaitTermination()
