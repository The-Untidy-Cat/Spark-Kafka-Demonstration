from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, LongType, BooleanType, TimestampType
from pyspark.sql.functions import from_json

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .option("startingOffsets", "earliest") \
    .load()

logs_schema = StructType([
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("device", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    StructField("status_code", LongType(), True),
    StructField("datetime", TimestampType(), True)
])

json_df = streaming_df.selectExpr("cast(value as string) as value")
json_expanded_df = json_df.withColumn("value", from_json(
    json_df["value"], logs_schema)).select("value.*")

query = json_expanded_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/parquet/files") \
    .option("checkpointLocation", "data/checkpoint") \
    .start()

query.awaitTermination()
