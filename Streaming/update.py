from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("mywordc count prohram") \
        .config("spark.sql.shuffle.partitions", 3) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.ui.port", 4545) \
        .getOrCreate()

## Read from socket
    inputDf = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "5555") \
        .load()

    ## Processing

    processdf = inputDf.withColumn("value", split(col("value"), " ")) \
        .withColumn("value", explode(col("value"))) \
        .groupby("value") \
        .count()


    ## Write to the console
    writedf = processdf.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "mycheckpoint2") \
        .start()

    writedf.awaitTermination()
