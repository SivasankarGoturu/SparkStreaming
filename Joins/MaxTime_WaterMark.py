
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, expr, col
from pyspark.sql.types import StructType, IntegerType, TimestampType, StringType
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("streaming to static") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 3) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema Declaration
impressionSchema = StructType() \
    .add("impressionID", IntegerType()) \
    .add("ImpressionTime", TimestampType()) \
    .add("CampaignName", StringType())

clisckSchema = StructType() \
    .add("clickID", IntegerType()) \
    .add("ClickTime", TimestampType())

# Data sources
impressionDf = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "5632") \
    .load()


clickDf = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "1234") \
    .load()

# Structure the data
valurDf = impressionDf.select(from_json(col("value"), impressionSchema).alias("value"))
impressionNewDf = valurDf.select("value.*").withWatermark("ImpressionTime", "30 minute")

valurDf2 = clickDf.select(from_json(col("value"), clisckSchema).alias("value"))
clickNewDf = valurDf2.select("value.*").withWatermark("ClickTime", "30 minute")

clickNewDf.printSchema()
impressionNewDf.printSchema()

joinCondition = expr("impressionNewDf['impressionID'] == clickNewDf['clickID'] AND ClickTime BETWEEN ImpressionTime AND ImpressionTime + interval 15 minute")
joinType = "inner"


joinDf = impressionNewDf.join(clickNewDf, joinCondition, joinType)
    #.drop(clickNewDf.clickID)

campainQuery = joinDf.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "ckLocation6") \
    .trigger(processingTime= '3 second') \
    .start()


campainQuery.awaitTermination()


