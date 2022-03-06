from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sys import stdin


spark = SparkSession.builder \
    .master("local[3]") \
    .appName("BeforeSalting") \
    .config("spark.ui.port", 9999) \
    .getOrCreate()

inputDf = spark.read \
    .format("csv") \
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/bigLogNew.txt") \
    .option("mode", "PERMISSIVE") \
    .load()


splitColumns = inputDf.withColumn("_c0", split(col("_c0"), ": ")) \
    .select(col("_c0").getItem(0).alias("Errortype"), col("_c0").getItem(1).alias("ErrorDescription"))

aggregations = splitColumns.groupBy("Errortype") \
    .agg(count("ErrorDescription"))


aggregations.show()


stdin.readline()



