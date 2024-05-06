# Create the Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, LongType, DateType, BooleanType
from pyspark.sql.functions import from_json

spark = SparkSession \
    .builder \
    .appName("Read from Parquet") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

df = spark.read.parquet("data/parquet/files")
df.show()
