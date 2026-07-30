import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "hpc-raw-metrics")

spark = (
    SparkSession.builder
    .appName("HPCMetricsConsumer")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)

schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("name", StringType(), True),
    StructField("fields", StructType([
        StructField("used_percent", DoubleType(), True),
        StructField("used", DoubleType(), True),
        StructField("total", DoubleType(), True),
        StructField("available", DoubleType(), True),
    ]), True),
    StructField("tags", StructType([
        StructField("cluster", StringType(), True),
        StructField("node_name", StringType(), True),
    ]), True),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

value_df = raw_df.selectExpr("CAST(value AS STRING) as value")

parsed_df = value_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

query = (
    parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
