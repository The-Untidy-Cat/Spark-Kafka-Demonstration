# Create the Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, LongType, DateType
from pyspark.sql.functions import from_json

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# consumer = consumer.subscribe("stocks", spark)

# Read the data from the Kafka topic
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .option("startingOffsets", "earliest") \
    .load()

logs_schema = StructType([
    StructField("browser", StringType(), True),
    StructField("status_code", LongType(), True),
    StructField("datetime", DateType(), True)
])

json_df = streaming_df.selectExpr("cast(value as string) as value")
json_expanded_df = json_df.withColumn("value", from_json(
    json_df["value"], logs_schema)).select("value.*")

query = json_expanded_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
