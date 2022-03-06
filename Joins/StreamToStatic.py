from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("streaming to static") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 3) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

inputSchema = StructType() \
    .add("card_id", LongType()) \
    .add("amount", IntegerType()) \
    .add("postcode", IntegerType()) \
    .add("pos_id", LongType()) \
    .add("transaction_dt", TimestampType())

transactionDf = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "5632") \
    .load()

valurDf = transactionDf.select(from_json(col("value"), inputSchema).alias("value"))
refinedDf = valurDf.select("value.*")



staticDf = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week16/dim_cards.csv") \
    .load()



joinCondition = staticDf.card_id == refinedDf.card_id
joinType = "inner"

joinDf = refinedDf.join(staticDf, joinCondition, joinType)
# .drop(staticDf.col("card_id"))

optdfq = joinDf.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "ck3") \
    .trigger(processingTime='3 seconds') \
    .start()

optdfq.awaitTermination()
