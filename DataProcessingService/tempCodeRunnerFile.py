
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("subscribe", "topic1") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")