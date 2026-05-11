from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = (
    SparkSession.builder
    .appName("NewsPulse")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("ts", TimestampType(), True)
])

stream = (
    spark.readStream
    .schema(schema)
    .json("data/incoming")
)

q_source = (
    stream.groupBy("source")
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("by_source")
    .start()
)

q_window = (
    stream.withWatermark("ts", "2 hours")
    .groupBy(F.window("ts", "1 hour"))
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("by_window")
    .start()
)

stop_words = [
    "the", "and", "for", "with", "from", "that", "this", "are", "was",
    "you", "your", "has", "have", "its", "but", "not", "after", "over",
    "new", "say", "says", "will", "can", "into", "about", "their"
]

words = (
    stream
    .select(F.explode(F.split(F.lower(F.regexp_replace("title", "[^a-zA-Z ]", "")), "\\s+")).alias("word"))
    .filter(~F.col("word").isin(stop_words))
    .filter(F.length("word") > 3)
)

q_words = (
    words.groupBy("word")
    .count()
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("top_words")
    .start()
)

spark.streams.awaitAnyTermination()
