import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSession").getOrCreate()
df= spark.read.csv('test.csv', header= True, inferSchema= True)
