from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("WikicorpusExp4Streaming").getOrCreate()
sc = spark.sparkContext

ssc = StreamingContext(sc, 1)  # 1-second batch interval

# Connect to the TCP server streaming preprocessed CSV lines
lines = ssc.socketTextStream("fa25-cs425-8501.cs.illinois.edu", 9999)

# Parse CSV lines into (docid, text)
parsed = lines.map(lambda line: line.strip().split(",", 1)).filter(lambda x: len(x) == 2)

# Filter documents containing "cat"
filtered = parsed.filter(lambda x: "cat" in x[1].lower())

# Reverse the text
transformed = filtered.map(lambda x: (x[0], x[1][::-1]))

# Save output incrementally
def save_rdd(rdd):
    if not rdd.isEmpty():
        rdd.map(lambda x: ",".join(x)).saveAsTextFile("/home/sameern3/spark_job_outputs/experiment4")

transformed.foreachRDD(save_rdd)

transformed.pprint()

ssc.start()
ssc.awaitTermination()
