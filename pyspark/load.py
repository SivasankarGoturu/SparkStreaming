from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import findspark
findspark.init()

sc = SparkContext("local", "AppName")
sql = SQLContext(sc)


df3 = pd.read_excel("C:/Users/gotur/Documents/SparkDataSets/electric-chargepoints2-2017.xlsx")

df4 = sql.createDataFrame(df3)

df4.show()

#df3 = sql.createDataFrame(df2)

#df3.show()

#openpyxl








"""
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("List to DataFrame converter") \
    .getOrCreate()
"""
