# Create the Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, LongType, DateType, BooleanType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col, countDistinct, date_trunc, count, current_date

spark = SparkSession \
    .builder \
    .appName("Read from Parquet") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

df = spark.read.parquet("data/parquet/files")
df.show()

#Thống kê số lượt truy cập lỗi trong ngày
def print_error_count_by_days(df):
    today = current_date().cast("date")
    error_count_today = df.filter((col("status_code") >= 400) & (col("datetime").cast("date") == today)).count()
    print(f"Số lượt truy cập lỗi trong ngày hôm nay: {error_count_today}")
    
def print_list_error_count_by_day(df):
    today = current_date().cast("date")
    error_accesses_today = df.filter((col("status_code") >= 400) & (col("datetime").cast("date") == today))
    print("Danh sách các lượt truy cập lỗi trong ngày hôm nay:")
    error_accesses_today.show(truncate=False)

def print_list_browser_access(df):
    browsers = df.select(col("browser")).distinct().rdd.map(lambda row: row[0]).collect()
    print("Danh sách các trình duyệt đã truy cập:")
    for browser in browsers:
        print(f"- {browser}")

def list_isBot(df):
    bot_accesses = df.filter(col("is_bot") == True)
    bot_accesses_list = bot_accesses.select("device").distinct().rdd.map(lambda row: row[0]).collect()
    print("Danh sách các lượt truy cập không phải từ trình duyệt:")
    for device in bot_accesses_list:
        print(f"- {device}")





