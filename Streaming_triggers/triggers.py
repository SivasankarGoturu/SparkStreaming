from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming import *
from pyspark import SparkConf
from pyspark.sql.functions import *

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

    # .withColumn("occrence", lit("1"))


    ## Time interval trigger
    writedf = processdf.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("checkpointLocation", "mycheckpoint11") \
        .trigger(processingTime='30 seconds') \
        .start()




    writedf.awaitTermination()
