from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming import *
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("mywordc count prohram") \
        .config("spark.sql.shuffle.partitions", 3) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.ui.port", 4545) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

## Read from socket
    inputDf = spark.readStream \
        .format("json") \
        .option("path", "C:/Users/gotur/PycharmProjects/SparkStreaming2/DataSources/InputFiles") \
        .load()
    #.option("maxFilesPerTrigger", 2) \
        ## Processing

    filteredDf = inputDf.filter(col("order_status") == "COMPLETE")

    # .withColumn("occrence", lit("1"))


    ## Time interval trigger
    writedf = filteredDf.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("checkpointLocation", "mycheckpoint70") \
        .option("path", "C:/Users/gotur/PycharmProjects/SparkStreaming2/DataSources/OutputFiles") \
        .trigger(processingTime='10 seconds') \
        .start()




    writedf.awaitTermination()
