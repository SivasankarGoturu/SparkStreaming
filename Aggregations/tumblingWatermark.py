from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("tumbling_with_water_mark") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

inputSchema = StructType() \
    .add("order_id", IntegerType()) \
    .add("order_date", TimestampType()) \
    .add("order_customer_id", IntegerType()) \
    .add("order_status", StringType()) \
    .add("amount", IntegerType())

inputDf = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "5632") \
    .load()


optDf = inputDf.select(from_json(col("value"), inputSchema).alias("value"))

df2 = optDf.select(col("value.*"))

windowDf = df2 \
    .withWatermark("order_date", "30 minute") \
    .groupBy(window(col("order_date"), "15 minute")) \
    .agg(sum("amount").alias("totalInvoice"))

oupDf = windowDf.select("window.start", "window.end", "totalInvoice")

outptQuery = oupDf.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "watermarkChecpoint6") \
    .trigger(processingTime = '3 seconds') \
    .start()


outptQuery.awaitTermination()