from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("TestSession").getOrCreate()

df= spark.read.csv('test.csv', header= True, inferSchema= True)

df= df.withColumnRenamed('CHQ.NO.', 'CHQ NO')
for each in df.schema.names:
    df = df.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))

df= df.select(df.columns[0:7])

df= df.withColumn('TransactionAmount', when(df._WITHDRAWAL_AMT_.isNull(), df._DEPOSIT_AMT_).otherwise(df._WITHDRAWAL_AMT_))
df= df.withColumn('TransactionType', when(df._WITHDRAWAL_AMT_.isNull(), 'CR').otherwise('DR'))

df.write.csv('outputPySpark', header= True)