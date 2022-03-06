from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, TimestampType, StringType

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
impressionNewDf = valurDf.select("value.*")

valurDf2 = clickDf.select(from_json(col("value"), clisckSchema).alias("value"))
clickNewDf = valurDf2.select("value.*")

clickNewDf.printSchema()
impressionNewDf.printSchema()

joinCondition = impressionNewDf.impressionID == clickNewDf.clickID
joinType = "inner"


joinDf = impressionNewDf.join(clickNewDf, joinCondition, joinType)
    #.drop(clickNewDf.clickID)

campainQuery = joinDf.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "ckLocation5") \
    .trigger(processingTime= '3 second') \
    .start()


campainQuery.awaitTermination()


