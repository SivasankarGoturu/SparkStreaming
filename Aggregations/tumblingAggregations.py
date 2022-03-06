from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("tumbling") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#.config("spark.sql.streaming.schemaInference", True) \

inputSchema = StructType() \
    .add("order_id", IntegerType()) \
    .add("order_date", TimestampType()) \
    .add("order_customer_id", IntegerType()) \
    .add("order_status", StringType()) \
    .add("amount", IntegerType())

#{"order_id":57012,"order_date":"2020-03-02 11:05:00","order_customer_id":2765,"order_status":"PROCESSING", "amount": 200}

inputDf = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 8888).load()

optDf = inputDf.select(from_json(col("value"), inputSchema).alias("value"))


df2 = optDf.select(col("value.*"))

windowDf = df2.groupBy(window(col("order_date"), "15 minute")) \
    .agg(sum("amount").alias("totalInvoice"))

oupDf = windowDf.select("window.start", "window.end", "totalInvoice")

ordersQuery = oupDf.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoint-location3") \
    .trigger(processingTime='3 seconds') \
    .start()

ordersQuery.awaitTermination()
